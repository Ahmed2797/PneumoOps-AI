from src.entity.config import Data_Ingestion_Config
from src.utils import read_yaml, create_directories
from src.exception import CustomException
from src.logger import logging
from src.constants import *
from pathlib import Path
import sys
import os




class Configeration_Manager:
    """
    Manages the loading and parsing of configuration and parameter YAML files.
    Creates required directories and provides configuration objects 
    for different pipeline components.
    """

    def __init__(self, config_filepath: Path = CONFIG_YAML_FILE, param_filepath: Path = PARAM_YAML_FILE):
        """
        Initialize the Configuration Manager.

        Args:
            config_filepath (Path): Path object for the main configuration YAML.
            param_filepath (Path): Path object for the parameters YAML.

        Raises:
            CustomException: If YAML reading or directory creation fails.
        """
        try:
            self.config = read_yaml(Path(config_filepath))
            self.param = read_yaml(Path(param_filepath))

            # create_directories expects a LIST of paths
            create_directories([self.config.artifacts_root])
            
            logging.info("Configuration Manager initialized and artifacts root created.")

        except Exception as e:
            raise CustomException(e, sys)


    def get_data_ingestion_config(self) -> Data_Ingestion_Config:
        """
        Creates and returns the Data_Ingestion_Config object 
        by reading values from the config YAML file.

        Returns:
            Data_Ingestion_Config: Configuration object for data ingestion.

        Steps:
            - Extract data ingestion section from config YAML
            - Ensure root directory exists
            - Populate Data_Ingestion_Config dataclass with YAML values

        Raises:
            CustomException: If extraction or object creation fails.
        """
        try:
            config = self.config.data_ingestion

            # Create root directory for data ingestion
            create_directories([config.root_dir])

            return Data_Ingestion_Config(
            root_dir=Path(config.root_dir),
            source_url=config.source_url,
            local_data_file=Path(config.local_data_file),
            unzip_dir=Path(config.unzip_dir),

            # 🔥 AWS fields
            bucket_name=config.bucket_name,
            object_key=config.object_key,
            region_name=config.region_name,
        )
        except Exception as e:
            raise CustomException(e, sys)
 