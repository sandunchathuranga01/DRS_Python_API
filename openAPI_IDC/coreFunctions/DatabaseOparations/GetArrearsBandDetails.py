"""
    Purpose:
    This module provides a function to retrieve the most recent active arrears band configuration
    from MongoDB.

    Description:
    - Connects to the MongoDB 'Arrears_bands' collection using config values.
    - Retrieves the latest document where `end_dtm` is empty or in the future.
    - Returns a merged dictionary of arrears band values.
    - Logs key actions and handles connection and query failures.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from pymongo import DESCENDING
from datetime import datetime, timezone
from pymongo import MongoClient
from openAPI_IDC.coreFunctions.ConfigManager import get_config
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Arrears Band Retrieval Function
def get_arrears_bands_details():
    """
    Retrieves the latest active arrears band configuration from the MongoDB 'Arrears_bands' collection.

    An arrears band is considered active if:
    - Its 'end_dtm' field is empty ("")
    - OR its 'end_dtm' is greater than or equal to the current UTC datetime

    Returns:
        dict: A dictionary of active arrears bands in the format {band_id: band_value, ...}
              Returns an empty dict if no active data is found.
              Returns False if database connection fails.
    """
    client = None
    try:
        # Get database configuration from the config hash map
        db_config = get_config("database", "DATABASE")

        # Connect to MongoDB using configured URI
        client = MongoClient(db_config.get("mongo_uri").strip())

        # Ping to verify the connection is alive
        client.admin.command('ping')

        # Select the configured database
        db = client[db_config.get("db_name").strip()]

    except Exception as e:
        # Log connection issues
        logger_INC1A01.error(f"Connection error: {e}")
        return {"success": False, "error": "Mongo DB connection error"}

    else:
        try:
            # Access the 'Arrears_bands' collection
            collection = db["Arrears_bands"]

            # Get the current UTC datetime
            current_date = datetime.now(timezone.utc)

            # Query to find documents where end_dtm is empty or in the future
            query = {
                "$or": [
                    {"end_dtm": {"$eq": ""}},               # No end date
                    {"end_dtm": {"$gte": current_date}}     # Future end date
                ]
            }

            # Find the most recently created active record
            document = collection.find_one(query, sort=[("create_dtm", DESCENDING)])

            # If document found and contains 'arrears_band' data
            if document and "arrears_band" in document:
                arrears_band_list = document["arrears_band"]

                # Merge list of dicts into one dict
                arrears_band_dict = {k: v for d in arrears_band_list for k, v in d.items()}

                logger_INC1A01.debug(f"Latest Active Arrears Band Data: {arrears_band_dict}")
                return arrears_band_dict
            else:
                logger_INC1A01.info("No active arrears band data found.")
                return {}

        except Exception as e:
            # Log query errors
            logger_INC1A01.error(f"Unexpected error in get_arrears_bands_details: {e}")
            return {}

    finally:
        # Clean up MongoDB connection
        if client:
            client.close()
# endregion
