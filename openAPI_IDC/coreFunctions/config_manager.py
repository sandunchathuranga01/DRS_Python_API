import configparser
import os
from utils.logger.loggers import get_logger

logger = get_logger("System_logger")

from utils.filePath.filePath import get_filePath

# Global dictionary to store hash maps
HASH_MAP = {}

# File paths
CONFIG_FILES = {
    "database": get_filePath("databaseConfig")
}

def read_ini_file(file_path):
    """Reads an INI file and returns a dictionary of its contents."""
    config = configparser.ConfigParser(interpolation=None)  # Disable interpolation
    config.read(file_path)
    data = {section: dict(config.items(section)) for section in config.sections()}
    return data

def initialize_hash_maps():
    """Creates hash maps from INI files at startup."""
    global HASH_MAP
    for name, path in CONFIG_FILES.items():
        if os.path.exists(path):
            HASH_MAP[name] = read_ini_file(path)
            logger.debug(f"Hash map for {name} is read.")
        else:
            logger.error(f"{path} not found. Skipping...")
            return False

def get_config(category, key=None):
    """Retrieves configuration values from the hash map."""
    if key:
        return HASH_MAP.get(category, {}).get(key, None)
    return HASH_MAP.get(category, {})


