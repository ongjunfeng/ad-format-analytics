import logging
import os
import pandas as pd
import requests
import instaloader
import boto3
from config import (
    INSTAGRAM_USERNAME, 
    INSTAGRAM_PASSWORD,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_BASE_PATH,
    S3_BUCKET_PATH
)

# Setup logging
logger = logging.getLogger(__name__)
class VideoProcessor:
    """
    A processor for processing Instagram Reels videos from URLs in pandas DataFrames.
    
    This class handles Instagram videos by first attempting to use direct media URLs
    (typically CDN links), and falling back to Instaloader to fetch fresh media URLs when the
    original URLs have expired. Optionally supports Instagram account login for accessing
    private accounts.
    
    Attributes:
        session (requests.Session): HTTP session for fetching videos.
        s3 (boto3.client): S3 client to interact with S3 bucket.
        loader (instaloader.Instaloader): Instaloader instance for fetching Instagram content.
    
    Example:
        >>> processor = VideoProcessor()
        >>> df = pd.DataFrame({
        ...     'post_id': ['ABC123'],
        ...     'post_url': ['https://www.instagram.com/p/ABC123/'],
        ...     'media_url': ['https://scontent-cdn.cdninstagram.com/...']
        ... })
        >>> result_df = processor.upload_video_df(df)
    """
    def __init__(self):
        """
        Initialize configured Instaloader instance.
        
        If INSTAGRAM_USERNAME and INSTAGRAM_PASSWORD are detected in the environment variables,
        an Instaloader instance with account login will be created, allowing access to private
        accounts on Instagram. Otherwise, some private account reels may not work with Instaloader.
        
        Environment Variables:
            INSTAGRAM_USERNAME (str, optional): Instagram account username for login.
            INSTAGRAM_PASSWORD (str, optional): Instagram account password for login.
        """
        self.session = requests.Session()
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=S3_BASE_PATH
        )
        loader = instaloader.Instaloader()
        if INSTAGRAM_USERNAME and INSTAGRAM_PASSWORD:
            try:
                loader.login(INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD)
                logger.info(f"Logged in to account: {INSTAGRAM_USERNAME}")
                self.loader = loader
                return
            except Exception as e:
                logger.error(f"Unable to login to Instagram Account due to {e}")
        self.loader = loader   
        logger.warning("Proceeding to use Instaloader without login")
        logger.warning("This may cause reels by private accounts to be inaccessible during instaloader fallback")

        return
    
    def _get_latest_medial_url(self, post_id: str) -> str:
        """
        Fetch the latest media CDN URL for an Instagram reel using Instaloader.
        
        Due to the temporary nature of Instagram CDN links (typically expiring within 1-2 days),
        this function is used as a fallback to retrieve a fresh media URL when the original
        URL has expired.

        Args:
            post_url (str): Instagram post URL, e.g. 'https://www.instagram.com/p/C8mtEPSp4b8/'
            
        Returns:
            str: Latest media CDN URL (e.g. 'https://scontent-atl3-3.cdninstagram...'),
                 or empty string if fetching fails.
        """
        L = self.loader
        try:
            post = instaloader.Post.from_shortcode(L.context, post_id)
        except Exception as e:
            logger.warning(f"❌ Failed to fetch post {post_id}: {e}")
            return ""
        return post.video_url

    def _upload_video(self, post_id: str, media_url: str) -> tuple:
        """
        Retrieve an Instagram video and upload it to S3 as an mp4 file.
        
        Attempts to read bytes content from the provided media_url first. If that fails (e.g., due to
        URL expiration), falls back to using Instaloader to fetch a fresh media URL and
        retries the upload.

        Args:
            post_id (str): Instagram reel ID, e.g. 'C8mtEPSp4b8'
            media_url (str): Direct media CDN URL, e.g. 'https://scontent-atl3-3.cdninstagram...'
            
        Returns:
            status_code (str): HTTP status code of the retrieval request (404 if both attempts fail).
            s3_upload_flag (bool): Boolean flag indicating if video has been successfully put in s3
                
        Side Effects:
            Uploads video to S3 using the post_id as the key
        """
        def upload_to_s3(post_id, media_url):
            """
            Gets the byte content from a media url and puts it in S3, returning the status code
            """
            response = self.session.get(media_url)
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()  
            video_bytes = response.content
            try:
                self.s3.put_object(
                    Bucket=S3_BUCKET_PATH,
                    Key=post_id,
                    Body=video_bytes,
                    ContentType="video/mp4"
                )
            except Exception as e:
                logger.error(f"Failed to put object in S3: {e}")
                return (response.status_code, False)
            return (response.status_code, True)

        try:
            # Try with main media URL
            logger.debug("Direct request without using instaloader")
            status_code, s3_upload_flag = upload_to_s3(post_id, media_url)
        except:
            try:
                # Fallback to latest media URL using instaloader
                logger.debug("Using instaloader to get latest media URL")
                latest_media_url = self._get_latest_medial_url(post_id)
                status_code, s3_upload_flag = upload_to_s3(post_id, latest_media_url)
            except:
                # Both methods unable to access instagram media URL
                status_code = 404
                s3_upload_flag = False
        return (status_code, s3_upload_flag)

    # ---------------- Main Function ----------------

    def upload_video_df(
        self, df: pd.DataFrame, post_id_col: str = "post_id", media_url_col: str = "media_url"
    ) -> pd.DataFrame:
        """
        Processes Instagram videos from a DataFrame and return results with S3 upload status.
        
        Processes each row in the DataFrame, uploading videos using the URLs provided in the
        specified columns. Adds two new columns to the DataFrame: 'status_code' (HTTP response
        status) and 'video_path' (local file path of uploaded video).

        Args:
            df (pd.DataFrame): DataFrame containing Instagram post and media URLs. Must include
                              a 'post_id' column for naming uploaded files.
            post_id_col (str, optional): Name of the column containing Instagram post IDs. 
                                         Defaults to "post_id".
            media_url_col (str, optional): Name of the column containing media CDN URLs. 
                                          Defaults to "media_url".
            
        Returns:
            pd.DataFrame: Copy of input DataFrame with an additional columns:
                - 'status_code': HTTP status code of video retrieval (404 if failed)
                - 's3_upload_flag': Boolean flag indicating if video was successfully uploaded to S3
                
        Side Effects:
            - Uploads video files to s3 with the key set as the post_id
            - Logs progress and failure information
        """
        df = df.copy()
        status_codes = []
        s3_upload_flags = []
        failed = 0
        uploaded = 0
        processed = 0
        for row in df.itertuples():
            status_code, s3_upload_flag = self._upload_video(
                post_id=getattr(row, post_id_col), 
                media_url=getattr(row, media_url_col)
            )
            status_codes.append(status_code)
            s3_upload_flags.append(s3_upload_flag)
            if not s3_upload_flag:
                failed += 1
                logger.warning(f"❌ Video {getattr(row, post_id_col)} failed to upload")
            else:
                uploaded += 1
            processed += 1
        
        df["status_code"] = status_codes
        df["s3_upload_flag"] = s3_upload_flags
        logger.info(f"🔍 {processed} videos processed")
        logger.info(f"✅ {uploaded} videos uploaded")
        logger.info(f"❌ {failed} videos failed to upload")
        return df