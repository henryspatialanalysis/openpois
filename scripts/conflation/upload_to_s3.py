"""
Upload the geohash-partitioned conflated POI dataset to a public S3 bucket.

Reads the partitioned dataset directory from config and uploads all parquet files to S3
with public-read ACL, preserving the Hive partition layout under a versioned S3 prefix:

    s3://<bucket>/<s3_prefix_conflation>/<aws_version>/conflated_partitioned/
        geohash_prefix=9q/part-0.parquet
        geohash_prefix=dr/part-0.parquet
        ...

Prerequisites — AWS setup (manual steps, not automated here):

1. Create S3 bucket in the AWS Console:
   - Choose a globally unique name (e.g. "openpois-public")
   - Uncheck "Block all public access" and acknowledge the warning
   - Leave other settings as defaults

2. Add a bucket policy for public GetObject access
   (S3 → your bucket → Permissions → Bucket policy):
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Sid": "PublicReadGetObject",
       "Effect": "Allow",
       "Principal": "*",
       "Action": "s3:GetObject",
       "Resource": "arn:aws:s3:::<bucket-name>/*"
     }]
   }

3. Create an IAM user with upload permissions
   (IAM → Users → Create user → attach inline policy):
   {
     "Effect": "Allow",
     "Action": ["s3:PutObject"],
     "Resource": "arn:aws:s3:::<bucket-name>/*"
   }
   Then generate access keys under Security credentials → Create access key
   (select "CLI" as use case).

4. Configure AWS credentials locally — pick one option:
   Option A (env vars):
     export AWS_ACCESS_KEY_ID=<your-key-id>
     export AWS_SECRET_ACCESS_KEY=<your-secret>
   Option B (AWS CLI):
     aws configure   (writes to ~/.aws/credentials)
"""
from config_versioned import Config

from openpois.io.s3 import upload_partitioned_dataset

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

PARTITIONED_DIR = config.get_file_path("conflation", "partitioned")
AWS_VERSION = config.get("versions", "aws")
S3_BUCKET = config.get("upload", "s3_bucket")
S3_PREFIX = config.get("upload", "s3_prefix_conflation")
S3_REGION = config.get("upload", "s3_region")


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    if not list(PARTITIONED_DIR.rglob("*.parquet")):
        raise FileNotFoundError(
            f"No parquet files found under {PARTITIONED_DIR}. "
            "Run format_for_upload.py first."
        )

    n = upload_partitioned_dataset(
        local_dir = PARTITIONED_DIR,
        bucket = S3_BUCKET,
        s3_prefix = S3_PREFIX,
        aws_version = AWS_VERSION,
        s3_region = S3_REGION,
    )
    base_url = (
        f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"
        f"/{S3_PREFIX}/{AWS_VERSION}/{PARTITIONED_DIR.name}/"
    )
    print(f"Uploaded {n:,} files.")
    print(f"Public base URL: {base_url}")
