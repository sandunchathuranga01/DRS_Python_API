"""
    Purpose:
    This module handles fetching and processing active F1 filter rules
    from the MongoDB 'F1_filter_config' collection.

    Description:
    - Retrieves active filters (based on `end_dtm`) and maps old IDs to new IDs.
    - Returns structured filter detail dictionaries.
    - Provides utility to get a list of all active `new_filter_id`s.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from pymongo import MongoClient, errors
from datetime import datetime, timezone
from openAPI_IDC.coreFunctions.ConfigManager import get_config, initialize_hash_maps
from utils.customerExceptions.cust_exceptions import DatabaseConnectionError, DataNotFoundError
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Get Active Filters
def get_active_filters():
    """
    Fetches all active F1 filter rules from the MongoDB collection 'F1_filter_config'.
    A filter is considered active if:
    - 'end_dtm' is an empty string
    - OR 'end_dtm' is a future datetime

    Returns:
        dict: A dictionary with original filter IDs as keys and filter details as values.
              Each value contains new_filter_id, filter_rule, operator, rule_values, and source_type.
    """
    # Load mapping from INI file (old_id -> new_id)
    filter_id_map = load_filter_id_mapping()
    filter_details = {}

    client = None
    try:
        # Fetch MongoDB connection configuration
        db_config = get_config("database", "DATABASE")
        client = MongoClient(db_config.get("mongo_uri").strip())
        client.admin.command('ping')  # Check DB connectivity
        db = client[db_config.get("db_name").strip()]

    except Exception as e:
        logger_INC1A01.error(f"Connection error: {e}")
        return {"success": False, "error": "Mongo DB connection error"}

    else:
        try:
            collection = db["F1_filter_config"]
            current_date = datetime.now(timezone.utc)

            # Query filters with no end_dtm or future end_dtm
            query = {
                "$or": [
                    {"end_dtm": {"$eq": ""}},
                    {"end_dtm": {"$gte": current_date}}
                ]
            }

            try:
                # Fetch all matching filter_documents
                filter_documents = collection.find(query)

                for doc in filter_documents:
                    filter_id = doc.get("filter_id")
                    new_filter_id = filter_id_map.get(filter_id)

                    if new_filter_id is None:
                        raise DataNotFoundError ("unknown filter_id found")

                    filter_rule = doc.get("filter_rule", "Unknown Rule")
                    operator = doc.get("operator", "Unknown Operator")
                    rule_values = [v.get("value") for v in doc.get("rule_values", [])]
                    source_type = [v.get("value") for v in doc.get("source_type", [])]

                    # Add filter details to dict
                    filter_details[filter_id] = {
                        "new_filter_id": new_filter_id,
                        "filter_rule": filter_rule,
                        "operator": operator,
                        "rule_values": rule_values,
                        "source_type": source_type,
                    }

                return filter_details

            except errors.PyMongoError as e:
                logger_INC1A01.error(f"MongoDB Query Error: {e}")
                return {}

        except errors.ServerSelectionTimeoutError:
            logger_INC1A01.error("Unable to connect to MongoDB server.")
            return {}

        except DataNotFoundError as e:
            logger_INC1A01.error(f"{e}")
            return {}

        except DatabaseConnectionError as database_connection_error:
            logger_INC1A01.error(f"MongoDB Connection Error: {database_connection_error}")
            return {}

        except Exception as e:
            logger_INC1A01.error(f"Unexpected Error: {e}")
            return {}

    finally:
        if client:
            client.close()
# endregion

# region Load Filter ID Mapping
def load_filter_id_mapping():
    """
    Loads filter ID mapping from the config file.
    Converts string keys and values to integers.

    Returns:
        dict: A mapping from old filter IDs to new filter IDs.
    """
    filter_rules = get_config("filter_rules")
    filter_id_map = {}

    for old_id_str, rule_data in filter_rules.items():
        try:
            old_id = int(old_id_str)
            new_id = int(rule_data["code_filter"])
            filter_id_map[old_id] = new_id
        except (ValueError, KeyError, TypeError):
            continue  # Skip invalid or missing entries

    return filter_id_map
# endregion

# region Get New Filter ID List
def get_new_filter_id_list_from_active_filters():
    """
    Retrieves a list of all new_filter_ids from the active filters.

    Returns:
        list: A list of integers (new_filter_id values)
    """
    try:
        active_filters = get_active_filters()
        return [details["new_filter_id"] for details in active_filters.values()]
    except Exception as e:
        logger_INC1A01.error(f"Error fetching new_filter_id list: {e}")
        return []
# endregion

if __name__ == "__main__":
    initialize_hash_maps()
    print(get_active_filters())