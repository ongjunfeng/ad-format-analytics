import os
import boto3
import pandas as pd
import requests
import instaloader
from config import AWS_CONFIG, INSTAGRAM_USERNAME

# -------------- USER CONFIG --------------
# excel_file = pd.read_excel("../datasets/instagram/cat_data_with_comments.xlsx")
# post_url_column = "Post URL"
s3_bucket = "mediaretrievalv1"
s3_prefix = "instagram_media/"
# -----------------------------------------

# Init S3 client
s3_client = boto3.client("s3", **AWS_CONFIG)

def upload_to_s3(content, key):
    s3_client.put_object(Body=content, Bucket=s3_bucket, Key=key)
    print(f"✅ Uploaded: s3://{s3_bucket}/{key}")

def get_instaloader():
    """Get configured Instaloader instance"""
    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False
    )
    
    # Session handling
    try:
        L.load_session_from_file(INSTAGRAM_USERNAME)
    except FileNotFoundError:
        L.interactive_login(INSTAGRAM_USERNAME)
        L.save_session_to_file()
    
    return L

# ---------------- Post Processing ----------------
def process_post(url, L=None):
    if L is None:
        L = get_instaloader()
    
    shortcode = url.strip("/").split("/")[-1]
    
    try:
        post = instaloader.Post.from_shortcode(L.context, shortcode)
    except Exception as e:
        print(f"❌ Failed to fetch post {url}: {e}")
        return

    if post.typename == "GraphSidecar":
        for idx, node in enumerate(post.get_sidecar_nodes(), start=1):
            if node.is_video:
                media_url = node.video_url
                ext, folder = ".mp4", "videos/"
            else:
                media_url = node.display_url or node.thumbnail_url
                ext, folder = ".jpg", "images/"

            filename = f"{shortcode}_{idx}{ext}"
            s3_key = os.path.join(s3_prefix, folder, filename)
            _download_and_upload(media_url, s3_key)
    else:
        if post.is_video:
            media_url = post.video_url
            ext, folder = ".mp4", "videos/"
        else:
            media_url = post.url
            ext, folder = ".jpg", "images/"

        filename = f"{shortcode}{ext}"
        s3_key = os.path.join(s3_prefix, folder, filename)
        _download_and_upload(media_url, s3_key)

def _download_and_upload(media_url, s3_key):
    try:
        resp = requests.get(media_url)
        if resp.status_code == 200:
            upload_to_s3(resp.content, s3_key)
        else:
            print(f"❌ Failed to fetch {media_url}, status {resp.status_code}")
    except Exception as e:
        print(f"❌ Error downloading {media_url}: {e}")

def process_dataframe(df: pd.DataFrame, post_url_column: str) -> None:
    for i, row in df.iterrows():
        url = row[post_url_column]
        if pd.isna(url):
            continue
        print(f"Processing row {i+1}: {url}")
        try:
            process_post(url)
        except Exception as e:
            print(f"❌ Error with {url}: {e}")


# ---------------- Main Script ----------------
def main():
    df = pd.read_excel(excel_file)
    for i, row in df.iterrows():
        url = row[post_url_column]
        if pd.isna(url):
            continue
        print(f"Processing row {i+1}: {url}")
        try:
            process_post(url)
        except Exception as e:
            print(f"❌ Error with {url}: {e}")

if __name__ == "__main__":
    main()
