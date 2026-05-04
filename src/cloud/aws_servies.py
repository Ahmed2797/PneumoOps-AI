import sys
import json
import boto3
from botocore.exceptions import ClientError
from src.exception import CustomException
from src.logger import logging


class S3Uploader:
    """
    A reusable AWS S3 utility class for ML pipelines.
    Supports upload, download, read, write, and object management.
    """

    def __init__(self, bucket_name: str, object_key: str = None):
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.s3 = boto3.client("s3")

    # =========================
    # ✅ UPLOAD FILE
    # =========================
    def upload_to_s3(self, file_path: str, object_key: str = None):
        try:
            key = object_key or self.object_key

            logging.info(f"Uploading {file_path} to s3://{self.bucket_name}/{key}")

            self.s3.upload_file(file_path, self.bucket_name, key)

            logging.info("Upload completed successfully!")

        except ClientError as e:
            logging.error("AWS upload error")
            raise CustomException(e, sys)

    # =========================
    # ✅ DOWNLOAD FILE
    # =========================
    def download_from_s3(self, local_path: str, object_key: str = None):
        try:
            key = object_key or self.object_key

            logging.info(f"Downloading s3://{self.bucket_name}/{key} to {local_path}")

            self.s3.download_file(self.bucket_name, key, local_path)

            logging.info("Download completed successfully!")

        except ClientError as e:
            logging.error("AWS download error")
            raise CustomException(e, sys)

    # =========================
    # ✅ READ FILE (TEXT / JSON)
    # =========================
    def read_file(self, object_key: str = None, as_json: bool = False):
        try:
            key = object_key or self.object_key

            logging.info(f"Reading file from s3://{self.bucket_name}/{key}")

            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            data = response["Body"].read()

            if as_json:
                return json.loads(data.decode("utf-8"))

            return data.decode("utf-8")

        except ClientError as e:
            logging.error("AWS read error")
            raise CustomException(e, sys)

    # =========================
    # ✅ WRITE STRING / JSON
    # =========================
    def write_file(self, data, object_key: str = None, as_json: bool = False):
        try:
            key = object_key or self.object_key

            logging.info(f"Writing file to s3://{self.bucket_name}/{key}")

            if as_json:
                data = json.dumps(data)

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data
            )

            logging.info("Write completed successfully!")

        except ClientError as e:
            logging.error("AWS write error")
            raise CustomException(e, sys)

    # =========================
    # ✅ CHECK IF FILE EXISTS
    # =========================
    def file_exists(self, object_key: str = None) -> bool:
        try:
            key = object_key or self.object_key

            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True

        except ClientError:
            return False

    # =========================
    # ✅ DELETE FILE
    # =========================
    def delete_file(self, object_key: str = None):
        try:
            key = object_key or self.object_key

            logging.info(f"Deleting s3://{self.bucket_name}/{key}")

            self.s3.delete_object(Bucket=self.bucket_name, Key=key)

            logging.info("Delete completed successfully!")

        except ClientError as e:
            logging.error("AWS delete error")
            raise CustomException(e, sys)

    # =========================
    # ✅ LIST FILES (PREFIX)
    # =========================
    def list_files(self, prefix: str = ""):
        try:
            logging.info(f"Listing files in s3://{self.bucket_name}/{prefix}")

            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            files = [obj["Key"] for obj in response.get("Contents", [])]

            return files

        except ClientError as e:
            logging.error("AWS list error")
            raise CustomException(e, sys)