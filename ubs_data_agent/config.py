import os
import argparse
import yaml 
from ubs_data_agent.logger import get_logger

DEFAULT_CONFIG_PATH = "/etc/config/ubs_data_agent.yaml"

logger = get_logger(__name__)


def load_config(config_path: str) -> dict:
    """
    Load configuration from a YAML file.

    :param config_path: Path to the configuration file.
    :return: Dictionary containing the configuration.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r") as file:
        try:
            config = yaml.safe_load(file)
            return config
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")


def get_config() -> dict:
    """
    Parse command-line arguments and load configuration.

    :return: Dictionary containing the configuration.
    """
    parser = argparse.ArgumentParser(description="Load configuration for the application.")
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to the configuration file (default: {DEFAULT_CONFIG_PATH})",
    )
    args = parser.parse_args()
    
    config_path = args.config
    logger.debug(f"Loading configuration from: {config_path}")
    return load_config(config_path)


