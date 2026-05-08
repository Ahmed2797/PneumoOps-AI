from pathlib import Path
import os
import cv2
import boto3
import numpy as np
import tensorflow as tf
from botocore.exceptions import ClientError
import matplotlib.pyplot as plt


BUCKET_NAME = "pneumonia-model-bucket"
MODEL_KEY = "best_chest_xray_model.keras"


class Prediction_Pipeline:

    def __init__(self, model_path: str):

        self.model_path = model_path

        # Download model if not exists
        self.download_s3()

        # Load model
        self.model = tf.keras.models.load_model(
            self.model_path,
            compile=False
        )

    def download_s3(self):

        try:

            if not os.path.exists(self.model_path):

                os.makedirs(
                    os.path.dirname(self.model_path),
                    exist_ok=True
                )

                print("Downloading model from S3...")

                s3 = boto3.client("s3")

                s3.download_file(
                    BUCKET_NAME,
                    MODEL_KEY,
                    self.model_path
                )

                print("Model downloaded successfully!")

            else:
                print("Model already exists.")

        except ClientError as e:
            print(f"AWS S3 Error: {e}")
            raise e

        except Exception as e:
            print(f"Error: {e}")
            raise e

    def preprocess_image(self, image_path: str):

        img = cv2.imread(image_path)

        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        original_shape = img.shape[:2]

        img_resized = cv2.resize(img, (256, 256))

        img_normalized = img_resized / 255.0

        img_final = np.expand_dims(img_normalized, axis=0)

        return img, img_final, original_shape

    def predict(self, image_path: str, threshold: float = 0.20):

        original_img, processed_img, (h, w) = self.preprocess_image(
            image_path
        )

        prediction = self.model.predict(processed_img)[0]

        prediction = np.squeeze(prediction)

        mask_256 = (prediction > threshold).astype(np.uint8)

        mask_resized = cv2.resize(
            mask_256,
            (w, h),
            interpolation=cv2.INTER_NEAREST
        )

        contours, _ = cv2.findContours(
            mask_resized.copy(),
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE
        )

        output_img = original_img.copy()

        for cnt in contours:

            if cv2.contourArea(cnt) > (w * h * 0.001):

                x, y, bw, bh = cv2.boundingRect(cnt)

                cv2.rectangle(
                    output_img,
                    (x, y),
                    (x + bw, y + bh),
                    (0, 255, 0),
                    3
                )

        return original_img, mask_resized, output_img


if __name__ == "__main__":
    # Demonstration of the prediction pipeline
    try:
        # pipeline = Prediction_Pipeline(MODEL_PATH="final_model/modelbest.keras")
        pipeline = Prediction_Pipeline(
            MODEL_PATH="final_model/best_chest_xray_model.keras"
        )
        test_img_path = "chest-xray/0_test_1_.png"

        orig, mask, result = pipeline.predict(test_img_path)

        # Visualization setup
        plt.figure(figsize=(16, 10))

        plt.subplot(2, 2, 1)
        plt.title("Original Chest X-ray", fontsize=12)
        plt.imshow(orig)
        plt.axis("off")

        plt.subplot(2, 2, 2)
        plt.title("AI Segmentation Mask", fontsize=12)
        plt.imshow(mask, cmap="gray")
        plt.axis("off")

        plt.subplot(2, 2, 3)
        plt.title("Detection Result (Bounding Box)", fontsize=12)
        plt.imshow(result)
        plt.axis("off")

        plt.subplot(2, 2, 4)
        plt.title("Highlighted Region (Red Overlay)", fontsize=12)
        plt.imshow(orig)
        plt.imshow(mask.squeeze(), cmap="Reds", alpha=0.4)
        plt.axis("off")
        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"Prediction failed with error: {e}")

## python src/components/inferance.py
