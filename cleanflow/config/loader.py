# cleanflow/config/loader.py

import yaml
import os

from cleanflow.utils.logger import get_logger

logger = get_logger(__name__)


def load_config(config_path: str) -> dict:
    """
    Load YAML configuration file.

    Args:
        config_path (str): Path to YAML config file

    Returns:
        dict: Parsed configuration
    """

    # Step 1: Check if file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    try:
        # Step 2: Open and read file
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        # Step 3: Handle empty config
        if config is None:
            raise ValueError("Config file is empty")

        logger.info(f"✅ Config loaded from {config_path}")

        return config

    except yaml.YAMLError as e:
        logger.error("❌ Failed to parse YAML config")
        raise e

    except Exception as e:
        logger.error(f"❌ Unexpected error loading config: {e}")
        raise e