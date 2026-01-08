"""
Script to extract first frame thumbnails from S3 videos and upload them back to S3.

This script:
1. Lists all .mp4 videos in specified S3 folders
2. Downloads each video temporarily
3. Extracts the first frame as a PNG thumbnail using ffmpeg
4. Uploads the thumbnail back to the same S3 folder
5. Cleans up temporary files
"""

import boto3
import subprocess
import os
import tempfile
import logging
from typing import List, Tuple
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# S3 Configuration
BUCKET_NAME = 'gtm-engine-media-dev'
BASE_PREFIX = 'nus/dataset_91qIDxbbk340bcKdW'
FOLDERS = ['viral', 'non_viral']


class ThumbnailGenerator:
    """Handles thumbnail generation from S3 videos."""

    def __init__(self, bucket_name: str):
        """Initialize with S3 bucket name."""
        self.bucket_name = bucket_name

        # Initialize S3 client with credentials from environment
        # If AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in .env,
        # boto3 will automatically use them
        self.s3_client = boto3.client('s3')

    def list_videos(self, prefix: str) -> List[str]:
        """
        List all .mp4 files in the specified S3 prefix.

        Args:
            prefix: S3 prefix/folder path

        Returns:
            List of S3 keys for .mp4 files
        """
        logger.info(f"Listing videos in s3://{self.bucket_name}/{prefix}")

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            video_keys = []
            for page in pages:
                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    key = obj['Key']
                    if key.lower().endswith('.mp4'):
                        video_keys.append(key)

            logger.info(f"Found {len(video_keys)} videos")
            return video_keys

        except Exception as e:
            logger.error(f"Error listing videos: {e}")
            raise

    def download_video(self, s3_key: str, local_path: str) -> None:
        """
        Download video from S3 to local path.

        Args:
            s3_key: S3 object key
            local_path: Local file path to save to
        """
        logger.info(f"Downloading {s3_key}")
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
        except Exception as e:
            logger.error(f"Error downloading {s3_key}: {e}")
            raise

    def extract_thumbnail(self, video_path: str, thumbnail_path: str) -> None:
        """
        Extract first frame from video as PNG thumbnail using ffmpeg.

        Args:
            video_path: Path to input video file
            thumbnail_path: Path to output thumbnail file
        """
        logger.info(f"Extracting thumbnail from {video_path}")

        try:
            # ffmpeg command to extract first frame
            # -i: input file
            # -vframes 1: extract only 1 frame
            # -vf scale=-1:720: scale to 720p height, maintain aspect ratio
            # -q:v 2: high quality (scale 2-31, lower is better)
            cmd = [
                'ffmpeg',
                '-i', video_path,
                '-vframes', '1',
                '-q:v', '2',
                '-y',  # Overwrite output file if exists
                thumbnail_path
            ]

            subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True
            )

            logger.info(f"Thumbnail created: {thumbnail_path}")

        except subprocess.CalledProcessError as e:
            logger.error(f"ffmpeg error: {e.stderr.decode()}")
            raise
        except Exception as e:
            logger.error(f"Error extracting thumbnail: {e}")
            raise

    def upload_thumbnail(self, local_path: str, s3_key: str) -> None:
        """
        Upload thumbnail to S3.

        Args:
            local_path: Local file path
            s3_key: S3 object key to upload to
        """
        logger.info(f"Uploading thumbnail to {s3_key}")

        try:
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'image/png'}
            )
            logger.info(f"Upload complete: s3://{self.bucket_name}/{s3_key}")

        except Exception as e:
            logger.error(f"Error uploading thumbnail: {e}")
            raise

    def check_thumbnail_exists(self, s3_key: str) -> bool:
        """
        Check if thumbnail already exists in S3.

        Args:
            s3_key: S3 object key to check

        Returns:
            True if thumbnail exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except:
            return False

    def process_video(self, video_key: str, skip_existing: bool = True) -> Tuple[bool, str]:
        """
        Process a single video: download, extract thumbnail, upload.

        Args:
            video_key: S3 key of the video
            skip_existing: Skip if thumbnail already exists

        Returns:
            Tuple of (success, message)
        """
        # Generate thumbnail S3 key (same path, .png extension)
        thumbnail_key = video_key.rsplit('.', 1)[0] + '.png'

        # Check if thumbnail already exists
        if skip_existing and self.check_thumbnail_exists(thumbnail_key):
            msg = f"Thumbnail already exists, skipping: {thumbnail_key}"
            logger.info(msg)
            return True, msg

        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Local file paths
                video_filename = os.path.basename(video_key)
                thumbnail_filename = os.path.basename(thumbnail_key)

                local_video = os.path.join(temp_dir, video_filename)
                local_thumbnail = os.path.join(temp_dir, thumbnail_filename)

                # Download video
                self.download_video(video_key, local_video)

                # Extract thumbnail
                self.extract_thumbnail(local_video, local_thumbnail)

                # Upload thumbnail
                self.upload_thumbnail(local_thumbnail, thumbnail_key)

                msg = f"Successfully processed: {video_key} -> {thumbnail_key}"
                logger.info(msg)
                return True, msg

            except Exception as e:
                msg = f"Failed to process {video_key}: {str(e)}"
                logger.error(msg)
                return False, msg

    def process_folder(self, folder_prefix: str, skip_existing: bool = True) -> dict:
        """
        Process all videos in a folder.

        Args:
            folder_prefix: S3 folder prefix
            skip_existing: Skip videos that already have thumbnails

        Returns:
            Dictionary with processing results
        """
        logger.info(f"Processing folder: {folder_prefix}")

        # List all videos
        video_keys = self.list_videos(folder_prefix)

        if not video_keys:
            logger.warning(f"No videos found in {folder_prefix}")
            return {
                'total': 0,
                'success': 0,
                'failed': 0,
                'skipped': 0,
                'results': []
            }

        # Process each video
        results = {
            'total': len(video_keys),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'results': []
        }

        for i, video_key in enumerate(video_keys, 1):
            logger.info(f"Processing video {i}/{len(video_keys)}: {video_key}")

            success, msg = self.process_video(video_key, skip_existing)

            if success:
                if 'already exists' in msg:
                    results['skipped'] += 1
                else:
                    results['success'] += 1
            else:
                results['failed'] += 1

            results['results'].append({
                'video': video_key,
                'success': success,
                'message': msg
            })

        return results


def main():
    """Main function to process all folders."""
    logger.info("Starting thumbnail generation process")
    logger.info(f"Bucket: {BUCKET_NAME}")
    logger.info(f"Base prefix: {BASE_PREFIX}")
    logger.info(f"Folders: {FOLDERS}")

    generator = ThumbnailGenerator(BUCKET_NAME)

    all_results = {}

    for folder in FOLDERS:
        folder_prefix = f"{BASE_PREFIX}/{folder}/"
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing folder: {folder}")
        logger.info(f"{'='*60}")

        results = generator.process_folder(folder_prefix, skip_existing=True)
        all_results[folder] = results

        logger.info(f"\nResults for {folder}:")
        logger.info(f"  Total videos: {results['total']}")
        logger.info(f"  Successfully processed: {results['success']}")
        logger.info(f"  Skipped (already exist): {results['skipped']}")
        logger.info(f"  Failed: {results['failed']}")

    # Final summary
    logger.info(f"\n{'='*60}")
    logger.info("FINAL SUMMARY")
    logger.info(f"{'='*60}")

    total_all = sum(r['total'] for r in all_results.values())
    success_all = sum(r['success'] for r in all_results.values())
    skipped_all = sum(r['skipped'] for r in all_results.values())
    failed_all = sum(r['failed'] for r in all_results.values())

    logger.info(f"Total videos across all folders: {total_all}")
    logger.info(f"Successfully processed: {success_all}")
    logger.info(f"Skipped: {skipped_all}")
    logger.info(f"Failed: {failed_all}")

    if failed_all > 0:
        logger.warning("\nFailed videos:")
        for folder, results in all_results.items():
            for result in results['results']:
                if not result['success']:
                    logger.warning(f"  {folder}/{result['video']}: {result['message']}")


if __name__ == '__main__':
    main()