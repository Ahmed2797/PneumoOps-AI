from dataclasses import dataclass 
from pathlib import Path 


@dataclass(frozen=True)
class Data_Ingestion_Config:
    """
    Dataclass for storing the configuration required to download and extract data.

    Attributes:
        root_dir (Path): Base directory for data artifacts.
        source_url (str): URL or S3 key from which to download the data.
        local_data_file (Path): Local path where the data will be saved.
        unzip_dir (Path): Directory where the data will be extracted.
        bucket_name (str): AWS S3 bucket name.
        object_key (str): S3 object key (path inside bucket).
        region_name (str): AWS region.
    """

    root_dir: Path
    source_url: str
    local_data_file: Path
    unzip_dir: Path

    # AWS fields
    bucket_name: str
    object_key: str
    region_name: str