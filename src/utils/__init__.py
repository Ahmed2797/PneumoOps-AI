import os
import sys
import json
import yaml
import base64
import joblib
import tensorflow as tf

from pathlib import Path
from typing import Any

from box import ConfigBox
from ensure import ensure_annotations

from src.logger import logging
from src.exception import CustomException


# ============================
# Y A M L   F U N C T I O N S
# ============================

@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """
    Reads a YAML file and returns its content as a ConfigBox.

    Args:
        path_to_yaml (Path): Path to the YAML file.

    Returns:
        ConfigBox: Parsed YAML content.

    Raises:
        CustomException: If file reading fails or content is invalid.
    """
    try:
        with open(path_to_yaml, "r") as yaml_file:
            content = yaml.safe_load(yaml_file)

        if content is None:
            raise ValueError(f"YAML file {path_to_yaml} is empty")

        logging.info(f"YAML file loaded: {path_to_yaml}")
        return ConfigBox(content)

    except Exception as e:
        raise CustomException(e, sys)


# ====================================
# D I R E C T O R I E S
# ====================================

@ensure_annotations
def create_directories(path_to_directories: list, verbose: bool = True):
    """
    Creates multiple directories if they do not exist.

    Args:
        path_to_directories (list): List of directory paths.
        verbose (bool): If True, logs directory creation.

    Raises:
        CustomException: If directory creation fails.
    """
    try:
        for path in path_to_directories:
            os.makedirs(path, exist_ok=True)
            if verbose:
                logging.info(f"Directory created: {path}")

    except Exception as e:
        raise CustomException(e, sys)


# =============================
# J S O N
# =============================

@ensure_annotations
def save_json(path: Path, data: dict):
    """
    Saves a dictionary as a JSON file.

    Args:
        path (Path): File path to save JSON.
        data (dict): Data to be saved.

    Raises:
        CustomException: If saving fails.
    """
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)

        logging.info(f"JSON saved: {path}")

    except Exception as e:
        raise CustomException(e, sys)


@ensure_annotations
def load_json(path: Path) -> ConfigBox:
    """
    Loads a JSON file and returns it as a ConfigBox.

    Args:
        path (Path): Path to JSON file.

    Returns:
        ConfigBox: Parsed JSON content.

    Raises:
        CustomException: If file reading fails.
    """
    try:
        with open(path, "r") as f:
            content = json.load(f)

        return ConfigBox(content)

    except Exception as e:
        raise CustomException(e, sys)


# ===============================
# B I N A R Y
# ===============================

@ensure_annotations
def save_bin(data: Any, path: Path):
    """
    Saves a Python object as a binary file using joblib.

    Args:
        data (Any): Object to save (e.g., ML model).
        path (Path): File path.

    Raises:
        CustomException: If saving fails.
    """
    try:
        joblib.dump(data, path)
        logging.info(f"Model saved: {path}")

    except Exception as e:
        raise CustomException(e, sys)


@ensure_annotations
def load_bin(path: Path) -> Any:
    """
    Loads a binary file using joblib.

    Args:
        path (Path): Path to binary file.

    Returns:
        Any: Loaded Python object.

    Raises:
        CustomException: If loading fails.
    """
    try:
        return joblib.load(path)

    except Exception as e:
        raise CustomException(e, sys)


# ==================
# U T I L S
# ==================

@ensure_annotations
def get_size(path: Path) -> str:
    """
    Returns file size in KB.

    Args:
        path (Path): File path.

    Returns:
        str: Human-readable file size.

    Raises:
        CustomException: If file not found or access fails.
    """
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} not found")

        size_in_kb = round(os.path.getsize(path) / 1024)
        return f"~ {size_in_kb} KB"

    except Exception as e:
        raise CustomException(e, sys)


# ===============================
# BASE64 IMAGE FUNCTIONS
# ===============================

@ensure_annotations
def decodeImage(imgstring: str, fileName: str) -> None:
    """
    Decodes a Base64 string and saves it as an image.

    Args:
        imgstring (str): Base64 encoded image string.
        fileName (str): Output image file path.

    Raises:
        CustomException: If decoding fails.
    """
    try:
        if not imgstring:
            raise ValueError("Empty image string")

        imgdata = base64.b64decode(imgstring)

        with open(fileName, "wb") as f:
            f.write(imgdata)

        logging.info(f"Image saved: {fileName}")

    except Exception as e:
        raise CustomException(e, sys)


@ensure_annotations
def encodeImageIntoBase64(croppedImagePath: str) -> bytes:
    """
    Encodes an image file into Base64 format.

    Args:
        croppedImagePath (str): Path to image file.

    Returns:
        bytes: Base64 encoded image.

    Raises:
        CustomException: If encoding fails.
    """
    try:
        with open(croppedImagePath, "rb") as f:
            return base64.b64encode(f.read())

    except Exception as e:
        raise CustomException(e, sys)


# ===============================
# TENSORFLOW DATA PIPELINE
# ===============================

def parse_data(img_path, mask_path):
    """
    Reads and preprocesses image and mask.

    Args:
        img_path: Path to image file.
        mask_path: Path to mask file.

    Returns:
        Tuple of (image, mask) tensors.
    """
    img = tf.io.read_file(img_path)
    img = tf.image.decode_png(img, channels=3)
    img = tf.image.resize(img, [256, 256])
    img = tf.cast(img, tf.float32) / 255.0

    mask = tf.io.read_file(mask_path)
    mask = tf.image.decode_png(mask, channels=1)
    mask = tf.image.resize(mask, [256, 256], method="nearest")
    mask = tf.cast(mask, tf.float32) / 255.0

    return img, mask


def sync_augment(img, mask):
    """
    Applies synchronized augmentation to image and mask.

    Args:
        img: Input image tensor.
        mask: Corresponding mask tensor.

    Returns:
        Augmented (image, mask) pair.
    """
    seed = tf.random.uniform([2], maxval=10000, dtype=tf.int32)

    img = tf.image.stateless_random_flip_left_right(img, seed=seed)
    mask = tf.image.stateless_random_flip_left_right(mask, seed=seed)

    img = tf.image.stateless_random_brightness(img, max_delta=0.2, seed=seed)

    return img, mask


def tf_dataset(x, y, batch_size=8, training=True):
    """
    Builds a TensorFlow dataset pipeline.

    Args:
        x: List of image paths.
        y: List of mask paths.
        batch_size (int): Batch size.
        training (bool): Whether to apply augmentation.

    Returns:
        tf.data.Dataset: Optimized dataset pipeline.
    """
    dataset = tf.data.Dataset.from_tensor_slices((x, y))

    dataset = dataset.map(parse_data, num_parallel_calls=tf.data.AUTOTUNE)

    if training:
        dataset = dataset.map(sync_augment, num_parallel_calls=tf.data.AUTOTUNE)
        dataset = dataset.shuffle(buffer_size=len(x), reshuffle_each_iteration=True)

    dataset = dataset.batch(batch_size)
    dataset = dataset.prefetch(tf.data.AUTOTUNE)

    return dataset