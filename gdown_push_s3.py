import os
import boto3
import gdown
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs
from botocore.exceptions import ClientError

from src.logger import logging
from src.exception import CustomException

load_dotenv()


class GDriveToS3Uploader:
    def __init__(self, bucket_name: str, object_key: str):
        try:
            self.bucket_name = bucket_name
            self.object_key = object_key

            self.s3 = boto3.client(
                "s3",
                region_name=os.getenv("AWS_DEFAULT_REGION")
            )

            logging.info("S3 client initialized successfully")

        except Exception as e:
            raise CustomException(e, sys)

    def _extract_file_id(self, gdrive_url: str) -> str:
        try:
            if "id=" in gdrive_url:
                file_id = parse_qs(urlparse(gdrive_url).query)["id"][0]
            else:
                file_id = gdrive_url.split("/")[-2]

            logging.info(f"Extracted file ID: {file_id}")
            return file_id

        except Exception as e:
            raise CustomException(e, sys)

    def download_from_gdrive(self, gdrive_url: str, output_path: str):
        try:
            file_id = self._extract_file_id(gdrive_url)

            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

            logging.info("Starting download from Google Drive...")

            output = gdown.download(
                f"https://drive.google.com/uc?id={file_id}",
                output_path,
                quiet=False
            )

            if output is None or not os.path.exists(output_path):
                raise Exception("Download failed!")

            logging.info(f"Download completed: {output_path}")

        except Exception as e:
            logging.error("Error during Google Drive download")
            raise CustomException(e, sys)

    def upload_to_s3(self, file_path: str):
        try:
            logging.info(f"Uploading file to S3 bucket: {self.bucket_name}")

            self.s3.upload_file(
                file_path,
                self.bucket_name,
                self.object_key
            )

            logging.info("Upload completed successfully!")

        except ClientError as e:
            logging.error("AWS ClientError occurred")
            raise CustomException(e, sys)

        except Exception as e:
            logging.error("Unexpected error during S3 upload")
            raise CustomException(e, sys)

    def run(self, gdrive_url: str, local_path: str):
        try:
            logging.info("🚀 Starting Google Drive → S3 pipeline")
            print("🚀 Starting Google Drive → S3 pipeline")
            
            self.download_from_gdrive(gdrive_url, local_path)
            self.upload_to_s3(local_path)

            logging.info("✅ AWS Pipeline completed successfully!")
            print("✅ AWS Pipeline completed successfully!")

        except Exception as e:
            logging.error("Pipeline failed")
            raise CustomException(e, sys)


# 🔥 Example usage
if __name__ == "__main__":
    import sys

    try:
        uploader = GDriveToS3Uploader(
            bucket_name="chest-xray-ahmed-2026",
            object_key="data/xray.zip"
        )

        uploader.run(
            gdrive_url="https://drive.google.com/file/d/1bo0OC0oT2o8lx7d5fBmVMEyOtBMMCBp2/view?usp=sharing",
            local_path="artifacts/data_ingestion/xray.zip"
        )

    except Exception as e:
        raise CustomException(e, sys)
    
## python gdown_push_s3.py