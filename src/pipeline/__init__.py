from pathlib import Path
import sys

from src.components.data_ingestion import Data_Ingestion
from src.components.prepare_basemodel import Prepare_Segmentation_Model

from src.configeration import Configeration_Manager
from src.exception import CustomException
from src.logger import logging


class Training_Pipeline:
    """
    Orchestrates the complete machine learning workflow including:
    - Data ingestion
    - Base model preparation
    - Callback creation
    - Model training
    - Model evaluation
    
    This class executes each pipeline step sequentially using configurations
    provided by the ConfigerationManager.
    """

    def __init__(self):
        """
        Initialize the TrainingPipeline with a single instance of
        ConfigerationManager to access all configuration sections.
        """
        self.config = Configeration_Manager()


    def run_data_ingestion(self):
        """
        Execute the data ingestion pipeline.
        
        Steps:
        - Download the dataset from the specified URL.
        - Extract the downloaded zip file.
        
        Raises:
            CustomException: If any part of ingestion fails.
        """
        try:
            logging.info(">>>>>>> Data Ingestion started <<<<<<<<<")
            data_ingestion_config = self.config.get_data_ingestion_config()
            data_ingestion = Data_Ingestion(data_ingestion_config)
            data_ingestion.run()
            logging.info(">>>>>>> Data Ingestion completed <<<<<<<<<")
        except Exception as e:
            raise CustomException(e, sys)
        
    

    def run_prepare_base_model(self):
        """
        Execute the base model preparation pipeline.
        
        Steps:
        - Build the ResNet50 U-Net model architecture.
        - Compile the model with the specified loss and metrics.
        - Save the prepared model to disk.
        
        Raises:
            CustomException: If any part of model preparation fails.
        """
        try:
            logging.info(">>>>>>> Base Model Preparation started <<<<<<<<<")
            prepare_base_model_config = self.config.get_prepare_base_model_config()
            model_preparer = Prepare_Segmentation_Model(config=prepare_base_model_config)
            unet_model = model_preparer.build_resnet50_unet()
            model_preparer.save_model(
                path=Path(prepare_base_model_config.update_base_model), 
                model=unet_model
            )
            logging.info(">>>>>>> Base Model Preparation completed <<<<<<<<<")
        except Exception as e:
            raise CustomException(e, sys)
        




    def run(self):
        """
        Execute the full ML pipeline in order:
        1. Data ingestion
        2. Base model preparation
        3. Model training
        4. Model evaluation
        
        Raises:
            CustomException: If any stage of the pipeline fails.
        """
        try:
            logging.info(">>>>>>> Training Pipeline started <<<<<<<<<")
            # self.run_data_ingestion()
            self.run_prepare_base_model()
        
            logging.info(">>>>>>> Training Pipeline completed <<<<<<<<<")
        except Exception as e:
            raise CustomException(e, sys)


