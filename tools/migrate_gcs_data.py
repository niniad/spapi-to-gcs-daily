import re
import argparse
from google.cloud import storage

# Configuration
DEST_BUCKET_NAME = "sp-api-bucket"

MIGRATION_CONFIGS = [
    {
        "source_bucket": "sp-api-sales-and-traffic-report-day",
        "filename_pattern": r"sp-api-sales-and-traffic-report-day-(\d{8})\.json",
        "dest_prefix": "sales-and-traffic-report/day/",
        "dest_filename_format": "{}.json"
    },
    {
        "source_bucket": "sp-api-sales-and-traffic-report-childasin",
        "filename_pattern": r"sp-api-sales-and-traffic-report-childasin-(\d{8})\.json",
        "dest_prefix": "sales-and-traffic-report/child-asin/",
        "dest_filename_format": "{}.json"
    }
]

def migrate_files(dry_run=True):
    """
    Migrates files from source buckets to destination bucket with renaming.
    """
    storage_client = storage.Client(project="main-project-477501")
    dest_bucket = storage_client.bucket(DEST_BUCKET_NAME)

    print(f"Starting migration to {DEST_BUCKET_NAME}...")
    if dry_run:
        print("[DRY RUN] No files will be actually copied or moved.")

    total_processed = 0
    total_copied = 0

    for config in MIGRATION_CONFIGS:
        source_bucket_name = config["source_bucket"]
        print(f"\nProcessing source bucket: {source_bucket_name}")
        
        try:
            source_bucket = storage_client.bucket(source_bucket_name)
            blobs = list(source_bucket.list_blobs()) # List all blobs
            
            print(f"  Found {len(blobs)} files.")

            for blob in blobs:
                match = re.match(config["filename_pattern"], blob.name)
                if match:
                    date_part = match.group(1)
                    new_filename = config["dest_filename_format"].format(date_part)
                    new_blob_name = f"{config['dest_prefix']}{new_filename}"
                    
                    print(f"  Processing: {blob.name} -> {new_blob_name}")
                    
                    if not dry_run:
                        # Copy the blob
                        source_bucket.copy_blob(blob, dest_bucket, new_blob_name)
                        print(f"    [COPIED]")
                        total_copied += 1
                    
                    total_processed += 1
                else:
                    print(f"  [SKIP] Pattern mismatch: {blob.name}")

        except Exception as e:
            print(f"  Error accessing bucket {source_bucket_name}: {e}")

    print("\n" + "="*30)
    print(f"Migration Complete.")
    print(f"Total files processed: {total_processed}")
    if not dry_run:
        print(f"Total files copied: {total_copied}")
    else:
        print(f"Total files that would be copied: {total_processed}")
    print("="*30)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate SP-API reports to new GCS bucket structure.")
    parser.add_argument("--execute", action="store_true", help="Execute the migration (disable dry-run)")
    args = parser.parse_args()

    migrate_files(dry_run=not args.execute)
