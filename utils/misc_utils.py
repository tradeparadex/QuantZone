"""
This module provides utility functions for various purposes.
"""

import ruamel.yaml


def load_config(file_path: str, raise_error: bool=True) -> dict:
    """
    Loads a YAML configuration file.

    Args:
        file_path (str): The path to the YAML configuration file.
        raise_error (bool): Whether to raise an error if the file is not found or invalid.

    Returns:
        dict: The loaded configuration.
    """
    try:
        with open(file_path, 'r') as file:
            return ruamel.yaml.safe_load(file)
    except Exception as e:
        if raise_error:
            raise e
        else:
            return {}