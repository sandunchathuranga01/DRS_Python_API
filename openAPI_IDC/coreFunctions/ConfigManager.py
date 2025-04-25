"""
    Purpose:
    Provides configuration management for the application by reading and storing
    values from INI files in a centralized hash map.

    Description:
    - Reads INI files (e.g., database config, filter rules).
    - Stores data in a global HASH_MAP for easy access.
    - Exposes utility functions to initialize configs and fetch specific values.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
import configparser
import os
from utils.logger.loggers import get_logger
from utils.filePath.filePath import get_filePath
# endregion

# region Logger
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Global Configuration
HASH_MAP = {}

CONFIG_FILES = {
    "database": get_filePath("databaseConfig"),
    "filter_rules": get_filePath("filterRuleConfig")
}
# endregion

# region Read INI File
def read_ini_file(file_path):
    """
    Reads an INI file and returns its contents as a nested dictionary.

    Args:
        file_path (str): Full path to the INI file.

    Returns:
        dict: Dictionary representation of the INI file (section → key/value pairs).
    """
    config = configparser.ConfigParser(interpolation=None)
    config.read(file_path)
    return {section: dict(config.items(section)) for section in config.sections()}
# endregion

# region Initialize Hash Maps
def initialize_hash_maps():
    """
    Initializes the global HASH_MAP with contents of all configured INI files.

    Returns:
        bool | None: Returns False if any config file is missing.
    """
    global HASH_MAP

    for name, path in CONFIG_FILES.items():
        if os.path.exists(path):
            HASH_MAP[name] = read_ini_file(path)
            logger_INC1A01.debug(f"Hash map for {name} is read.")
        else:
            logger_INC1A01.error(f"{path} not found. Skipping...")
            return False
# endregion

# region Get Configuration
def get_config(category, key=None):
    """
    Retrieves configuration values from the loaded HASH_MAP.

    Args:
        category (str): The category/section of config to retrieve (e.g., 'database', 'filter_rules').
        key (str, optional): Specific key to retrieve within the category.

    Returns:
        dict | str | None: Full category dict if key is None, otherwise the specific config value.
    """
    if key:
        return HASH_MAP.get(category, {}).get(key, None)
    return HASH_MAP.get(category, {})
# endregion
