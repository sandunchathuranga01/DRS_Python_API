import configparser
from pymongo import errors
from datetime import datetime, timezone

from openAPI_IDC.coreFunctions.config_manager import initialize_hash_maps
from utils.customerExceptions.cust_exceptions import DatabaseConnectionError
from utils.database.connectDB import get_db_connection
from utils.filePath.filePath import get_filePath

from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')


def load_filter_id_mapping():

    ini_file_path = get_filePath("filterRuleConfig")
    config = configparser.ConfigParser()
    config.read(ini_file_path)

    filter_id_map = {}
    for section in config.sections():
        try:
            old_id = int(section)
            new_id = int(config[section]['code_filter'])
            filter_id_map[old_id] = new_id
        except (ValueError, KeyError):
            continue

    return filter_id_map


def get_active_filters():
    """
    Connects to MongoDB, retrieves active filters, and maps filter_id to new_filter_id using INI config.
    :return: Dictionary of filters with new_filter_id
    """
    filter_hash_map = {}
    db = False

    try:
        db = get_db_connection()

        if db is False:
            raise DatabaseConnectionError("could not connect to MongoDB")

        collection = db["F1_filter_config"]

        # Get current UTC date
        current_date = datetime.now(timezone.utc)

        # Query: Retrieve filters where 'end_dtm' is empty or in the future
        query = {
            "$or": [
                {"end_dtm": {"$eq": ""}},  # Empty end_dtm
                {"end_dtm": {"$gte": current_date}}  # Future end_dtm
            ]
        }

        # Load mapping from INI file
        filter_id_map = load_filter_id_mapping()

        try:
            # Fetch documents from MongoDB
            documents = collection.find(query)

            for doc in documents:
                filter_id = doc.get("filter_id")
                new_filter_id = filter_id_map.get(filter_id)

                if new_filter_id is None:
                    continue  # skip if no mapping found

                filter_rule = doc.get("filter_rule", "Unknown Rule")
                operator = doc.get("operator", "Unknown Operator")
                rule_values = [value.get("value") for value in doc.get("rule_values", [])]
                source_type = [value.get("value") for value in doc.get("source_type", [])]

                # Store in hash map with new_filter_id
                filter_hash_map[filter_id] = {
                    "new_filter_id": new_filter_id,
                    "filter_rule": filter_rule,
                    "operator": operator,
                    "rule_values": rule_values,
                    "source_type": source_type,
                }

            return filter_hash_map

        except errors.PyMongoError as e:
            logger_INC1A01.error(f"MongoDB Query Error: {e}")


    except errors.ServerSelectionTimeoutError:
        logger_INC1A01.error("Error: Unable to connect to MongoDB. Please check your connection.")
        return {}

    except DatabaseConnectionError as database_connection_error:
        logger_INC1A01.error(f"MongoDB Connection Error: {database_connection_error}")
        return {}

    except Exception as e:
        logger_INC1A01.error(f"Unexpected Error: {e}")
        return {}

    finally:
        if db is not False:
            db.client.close()

def get_new_filter_id_list_from_active_filters():
    try:
        """
        Retrieves the list of new_filter_id values from the active filters.
        :return: List of new_filter_id integers
        """
        active_filters = get_active_filters()
        return [details["new_filter_id"] for details in active_filters.values()]
    except Exception as e:
        logger_INC1A01.error(f"Error: {e}")
        return []

def get_line_by_new_filter_id(target_new_filter_id):
    filters = get_active_filters()

    for data in filters.values():
        if data.get("new_filter_id") == target_new_filter_id:
            return data

    return False



if __name__ == "__main__":
    initialize_hash_maps()
    # print(get_active_filters())
    print(get_new_filter_id_list_from_active_filters())
    # filter_01_details = get_line_by_new_filter_id(11)
    # print(filter_01_details)


#output this main
{1: {'new_filter_id': 11, 'filter_rule': 'BSS Credit Class', 'operator': 'equal', 'rule_values': [3, 7, 10, 43], 'source_type': []},
 2: {'new_filter_id': 12, 'filter_rule': 'Customer Type Name', 'operator': 'equal', 'rule_values': ['SLT'], 'source_type': []},
 3: {'new_filter_id': 13, 'filter_rule': 'Product Status', 'operator': 'equal', 'rule_values': ['OK'], 'source_type': []},
 4: {'new_filter_id': 14, 'filter_rule': 'Customer Segment', 'operator': 'equal', 'rule_values': [2, 4, 6, 7], 'source_type': []},
 5: {'new_filter_id': 15, 'filter_rule': 'Specific Customer Name', 'operator': 'like', 'rule_values': ['Banks', 'Brandix', 'MAS', 'Mobitel', 'Hutch', 'Etisalat', 'Airtel', 'Lanka Bell', 'Dialog', 'Suntel'], 'source_type': []},
 6: {'new_filter_id': 16, 'filter_rule': 'Specific Product Status', 'operator': 'equal', 'rule_values': ['SU'], 'source_type': ['special' ,'pilot_suspend']},
 }

[11, 12, 13, 14, 15]



