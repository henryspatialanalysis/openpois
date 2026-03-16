"""
Upload the geohash-partitioned OSM snapshot to a public S3 bucket.

Reads the partitioned dataset directory from config and uploads all parquet
files to S3 with public-read ACL, preserving the Hive partition layout under
a versioned S3 prefix:

    s3://<bucket>/<s3_prefix>/<aws_version>/osm_snapshot_partitioned/
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
from pathlib import Path

import boto3
from config_versioned import Config
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

PARTITIONED_DIR = config.get_file_path("snapshot_osm", "partitioned")
AWS_VERSION = config.get("versions", "aws")
S3_BUCKET = config.get("upload", "s3_bucket")
S3_PREFIX = config.get("upload", "s3_prefix")
S3_REGION = config.get("upload", "s3_region")


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


def upload_partitioned_dataset(
    local_dir: Path,
    bucket: str,
    s3_prefix: str,
    aws_version: str,
    s3_region: str,
) -> int:
    """Upload all parquet files in local_dir to S3 with public-read ACL.

    S3 key structure:
        {s3_prefix}/{aws_version}/{dataset_dir_name}/{hive_partitions}/part-N.parquet

    The dataset directory name (e.g. osm_snapshot_partitioned) is preserved in
    the key path by computing relative paths from local_dir.parent.

    Returns the number of files uploaded.
    """
    s3 = boto3.client("s3", region_name = s3_region)
    parquet_files = sorted(local_dir.rglob("*.parquet"))

    print(f"Uploading {len(parquet_files):,} files to s3://{bucket}/{s3_prefix}/")
    for local_path in tqdm(parquet_files, desc = "Uploading to S3", unit = "file"):
        relative = local_path.relative_to(local_dir.parent)
        s3_key = f"{s3_prefix}/{aws_version}/{relative}"
        s3.upload_file(
            Filename = str(local_path),
            Bucket = bucket,
            Key = s3_key,
        )

    return len(parquet_files)


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
