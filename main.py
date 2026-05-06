import sys

from src.exception import CustomException
from src.pipeline import Training_Pipeline

if __name__ == "__main__":
    try:
        training_pipeline = Training_Pipeline()
        training_pipeline.run()
    except Exception as e:
        raise CustomException(e, sys)
