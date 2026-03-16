#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
Utilities for uploading local datasets to S3.
"""
from pathlib import Path

import boto3
from tqdm import tqdm


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
