"""
S3 Service for downloading and managing media files
"""
import os
import boto3
from pathlib import Path
from typing import List, Dict, Optional
from config import AWS_CONFIG

class S3Service:
    def __init__(self, bucket: str = "mediaretrievalv1", prefix: str = "instagram_media/"):
        self.s3_client = boto3.client("s3", **AWS_CONFIG)
        self.bucket = bucket
        self.prefix = prefix
        
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a single file from S3 to local path.
        
        Args:
            s3_key: S3 object key (e.g., 'instagram_media/videos/ABC123.mp4')
            local_path: Local file path to save to
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create parent directory if it doesn't exist
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(self.bucket, s3_key, local_path)
            print(f"✅ Downloaded: s3://{self.bucket}/{s3_key} -> {local_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to download {s3_key}: {e}")
            return False
    
    def download_video_by_post_id(self, post_id: str, local_dir: str = "downloads") -> Optional[str]:
        """
        Download a video from S3 using Instagram post ID.
        
        Args:
            post_id: Instagram post shortcode (e.g., 'C8mtEPSp4b8')
            local_dir: Local directory to save to
            
        Returns:
            Local file path if successful, None otherwise
        """
        # Try common video patterns
        video_patterns = [
            f"{self.prefix}videos/{post_id}.mp4",
            f"{self.prefix}videos/{post_id}_1.mp4",
        ]
        
        local_path = os.path.join(local_dir, f"{post_id}.mp4")
        
        for s3_key in video_patterns:
            if self.file_exists(s3_key):
                if self.download_file(s3_key, local_path):
                    return local_path
        
        print(f"⚠️ Video not found in S3 for post_id: {post_id}")
        return None
    
    def file_exists(self, s3_key: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except:
            return False
    
    def list_videos(self, prefix_filter: str = None) -> List[str]:
        """
        List all video files in S3 bucket.
        
        Args:
            prefix_filter: Optional prefix to filter (e.g., 'instagram_media/videos/')
            
        Returns:
            List of S3 keys
        """
        prefix = prefix_filter or f"{self.prefix}videos/"
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.mp4')]
        except Exception as e:
            print(f"❌ Error listing S3 objects: {e}")
            return []
    
    def download_batch(self, post_ids: List[str], local_dir: str = "downloads") -> Dict[str, str]:
        """
        Download multiple videos by post IDs.
        
        Args:
            post_ids: List of Instagram post shortcodes
            local_dir: Local directory to save to
            
        Returns:
            Dictionary mapping post_id -> local_path (only successful downloads)
        """
        results = {}
        
        for post_id in post_ids:
            local_path = self.download_video_by_post_id(post_id, local_dir)
            if local_path:
                results[post_id] = local_path
        
        print(f"✅ Downloaded {len(results)}/{len(post_ids)} videos from S3")
        return results
    
    def get_s3_uri(self, post_id: str) -> str:
        """Get full S3 URI for a post."""
        return f"s3://{self.bucket}/{self.prefix}videos/{post_id}.mp4"
    
    def delete_local_downloads(self, local_dir: str = "downloads"):
        """Clean up local download directory."""
        import shutil
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
            print(f"🗑️ Cleaned up {local_dir}")