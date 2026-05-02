import os
import zipfile
import boto3
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from botocore.exceptions import ClientError

from src.logger import logging
from src.exception import CustomException
from src.utils import get_size
from src.entity.config import Data_Ingestion_Config
import sys

load_dotenv()


class Data_Ingestion:
    def __init__(self, config: Data_Ingestion_Config):
        try:
            self.config = config
        except Exception as e:
            raise CustomException(e, sys)

    def _make_s3(self):
        try:
            """
            Create S3 client using environment variables or IAM role
            """
            return boto3.client(
                "s3",
                region_name=self.config.region_name
            )
        except Exception as e:
            raise CustomException(e, sys)

    def _stream_download(self, s3, bucket_name, object_key, download_path):
        try:
            Path(download_path).parent.mkdir(parents=True, exist_ok=True)

            logging.info(f"Downloading {object_key} from bucket {bucket_name}...")

            response = s3.get_object(
                Bucket=bucket_name,
                Key=object_key,
            )

            body = response["Body"]

            tmp_dir = Path(download_path).parent
            with tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp_f:
                tmp_path = tmp_f.name

                for chunk in body.iter_chunks(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        tmp_f.write(chunk)

            body.close()

            os.replace(tmp_path, download_path)

            logging.info("Download completed successfully")
            logging.info(f"File size: {get_size(Path(download_path))}")

        except Exception as e:
            raise CustomException(e, sys)

    def download_from_s3(self):
        try:
            s3 = self._make_s3()

            self._stream_download(
                s3,
                self.config.bucket_name,
                self.config.object_key,
                self.config.local_data_file,
            )

        except ClientError as e:
            code = e.response["Error"].get("Code", "")
            msg = e.response["Error"].get("Message", str(e))

            logging.error(f"AWS Error ({code}): {msg}")

            if code in ["403", "AccessDenied"]:
                logging.error("Check IAM permissions for S3 access")
                raise CustomException(e, sys)

            elif code in ["PermanentRedirect", "301", "AuthorizationHeaderMalformed"]:
                try:
                    loc = s3.get_bucket_location(
                        Bucket=self.config.bucket_name
                    )["LocationConstraint"]

                    retry_region = loc or "us-east-1"

                    logging.info(f"Retrying with correct region: {retry_region}")

                    s3_retry = boto3.client("s3", region_name=retry_region)

                    self._stream_download(
                        s3_retry,
                        self.config.bucket_name,
                        self.config.object_key,
                        self.config.local_data_file,
                    )

                except Exception as e2:
                    raise CustomException(e2, sys)
            else:
                raise CustomException(e, sys)

        except Exception as e:
            raise CustomException(e, sys)

    def extract_zip_file(self):
        try:
            unzip_path = Path(self.config.unzip_dir)
            unzip_path.mkdir(parents=True, exist_ok=True)

            logging.info("Extracting zip file...")

            with zipfile.ZipFile(self.config.local_data_file, "r") as zip_ref:
                zip_ref.extractall(unzip_path)

            logging.info(f"Extraction completed at {unzip_path}")

        except Exception as e:
            raise CustomException(e, sys)

    def run(self):
        try:
            logging.info("Starting Data Ingestion Pipeline")

            self.download_from_s3()
            self.extract_zip_file()

            logging.info("Data Ingestion Completed Successfully")

        except Exception as e:
            raise CustomException(e, sys)