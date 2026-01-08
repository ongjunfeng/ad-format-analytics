import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# from config import (
#     AWS_ACCESS_KEY_ID,
#     AWS_SECRET_ACCESS_KEY,
#     S3_BASE_PATH,
#     S3_BUCKET_PATH
# )


sbucket_path = "gtm-engine-media-dev"
base_path = "https://s3.amazonaws.com"
access_key_id = "ACCESS_KEY_HERE"
secret_access_key = "SECRET_KEY_HERE"
test_key = "test_s3_write.txt"
# test_content = "Hello, World!"

# print(AWS_ACCESS_KEY_ID == access_key_id)
# print(AWS_SECRET_ACCESS_KEY == secret_access_key)
# print(S3_BASE_PATH == base_path)
# print(S3_BUCKET_PATH == sbucket_path)

import requests
res = requests.get("http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4")
test_content = res.content

# --- INITIALIZE CLIENT ---
s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    endpoint_url=base_path
)

try:
    print(f"--- Testing full S3 access for bucket: {sbucket_path} ---")

    # 1️⃣ WRITE
    print("Uploading test file...")
    s3.put_object(Bucket=sbucket_path, Key=test_key, Body=test_content, ContentType="video/mp4")
    print("✅ File uploaded successfully.")

    # 2️⃣ READ
    print("Reading test file...")
    response = s3.get_object(Bucket=sbucket_path, Key=test_key)
    body = response["Body"].read()
    with open("test.mp4", "wb") as f:
        f.write(body)
    print("✅ File downloaded successfully.")
    if body == test_content:
        print(f"✅ File content verified")
    else:
        print(f"⚠️ Content mismatch!")

    # 3️⃣ DELETE
    print("Deleting test file...")
    s3.delete_object(Bucket=sbucket_path, Key=test_key)
    print("✅ File deleted successfully.")

    print("\n🎯 S3 bucket read/write/delete test completed successfully!")

except ClientError as e:
    print(f"❌ AWS ClientError: {e}")
except (NoCredentialsError, PartialCredentialsError):
    print("❌ Missing or incomplete AWS credentials.")
except Exception as e:
    print(f"❌ Unexpected error: {e}")