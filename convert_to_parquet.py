import dask.dataframe as dd
import pandas as pd
import boto3
import os
from pathlib import Path
import shutil

# Configuration
CSV_DIR = "/mnt/data/csv_files"
PARQUET_DIR = "/mnt/data/parquet_files"
S3_BUCKET = "data228"
S3_PREFIX = "Data_2020_2024"

# Create parquet directory if it doesn't exist
os.makedirs(PARQUET_DIR, exist_ok=True)

# List all CSV files
csv_files = sorted([f for f in os.listdir(CSV_DIR) if f.endswith('.csv')])
print(f"Found {len(csv_files)} CSV files to process:")
for file in csv_files:
    print(f"- {file}")

# Process each file
for csv_file in csv_files:
    csv_path = os.path.join(CSV_DIR, csv_file)
    filename = Path(csv_path).stem
    parquet_dir = os.path.join(PARQUET_DIR, f"{filename}.parquet")
    
    print(f"\nProcessing {filename}")
    print(f"Input file size: {os.path.getsize(csv_path) / (1024 * 1024 * 1024):.2f} GB")
    
    try:
        # Read CSV using dask
        print("Reading CSV file with dask...")
        ddf = dd.read_csv(csv_path)
        
        # Convert to parquet (this creates a directory)
        print("Converting to Parquet...")
        ddf.to_parquet(parquet_dir, compression='snappy')
        
        # Combine all parquet parts and upload as one file
        print(f"Uploading combined parquet file to S3...")
        s3_client = boto3.client('s3')
        s3_key = f"{S3_PREFIX}/{filename}.parquet"
        
        # Get all parts
        parts = [os.path.join(parquet_dir, f) for f in os.listdir(parquet_dir)]
        
        # Read and combine all parts
        ddf_combined = dd.read_parquet(parts)
        combined_file = os.path.join(PARQUET_DIR, f"{filename}_combined.parquet")
        ddf_combined.compute().to_parquet(combined_file, compression='snappy')
        
        # Upload combined file
        s3_client.upload_file(combined_file, S3_BUCKET, s3_key)
        print(f"Successfully uploaded to s3://{S3_BUCKET}/{s3_key}")
        
        # Cleanup
        shutil.rmtree(parquet_dir)
        os.remove(combined_file)
        print("Removed local parquet files to save space")
        
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")
        continue

print("\nAll files processed successfully!")