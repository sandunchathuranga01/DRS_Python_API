"""
    Purpose:
    This module provides utility functions to check for open cases based on account information
    and link related accounts to incidents in the MongoDB database.

    Description:
    - `has_open_case_for_account`: Verifies if the given Account_Num has any non-closed case.
    - `link_accounts_from_open_cases`: Finds all open accounts sharing the same customer_ref
      and adds them to the `Link_Accounts` list in the incident_dict.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from pymongo import MongoClient
from openAPI_IDC.coreFunctions.ConfigManager import get_config, initialize_hash_maps
from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from utils.logger.loggers import get_logger
from datetime import datetime
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Global Variables
customer_link_accounts_details = []
# endregion

# region has_open_case_for_account
def has_open_case_for_account(incident_dict):
    """
        Checks if there is at least one **open case** for the given Account_Num
        in the 'Case_details' collection.

        A case is considered "open" if the case_current_status is NOT in
        ["close", "write-off", "abandoned", "withdraw"], ignoring case sensitivity.

        Args:
            incident_dict (dict): A dictionary that must contain the key 'Account_Num'.

        Returns:
            "True"  -> if at least one open case is found.
            "False" -> if no open cases are found or if any error occurs.
        """

    client = None
    try:
        # Load MongoDB configuration
        db_config = get_config("database", "DATABASE")

        # Connect to MongoDB
        client = MongoClient(db_config.get("mongo_uri").strip())
        client.admin.command('ping')  # Ensure MongoDB is reachable

        # Get database and collection
        db = client[db_config.get("db_name").strip()]

    except Exception as err:
        # Handle any errors that occur during connection setup
        logger_INC1A01.info(f"Connection error: {err}")
        return "False"

    else:
        try:
            collection = db["Case_details"]

            # Define the query to check for open cases
            query = {
                "$expr": {
                    "$and": [
                        {"$eq": ["$Account_Num", incident_dict.get("Account_Num")]},
                        {
                            "$not": {
                                "$in": [
                                    {"$toLower": "$case_current_status"},
                                    ["close", "write-off", "abandoned", "withdraw"]
                                ]
                            }
                        }
                    ]
                }
            }

            # Check if any matching document exists
            Open_acc = collection.find_one(query)

            logger_INC1A01.debug(Open_acc)

            if Open_acc:
                logger_INC1A01.info("open cases found for relevant account")
                return "True"

            else:
                logger_INC1A01.info("No open cases found for relevant account")
                return "False"


        except Exception as e:
            logger_INC1A01.error(f"Error: {e}")
            return "False"

    finally:
        # Close the MongoDB client connection
        if client:
            client.close()


# endregion

# region Link Accounts Function
def link_accounts_from_open_accounts(incident_dict):
    logger_INC1A01.info("Started linking accounts from open incidents and cases.")
    customer_link_accounts_details.clear()
    try:
        customer_ref = incident_dict['Customer_Ref']
        logger_INC1A01.debug(f"Customer Ref: {customer_ref}")

        root_account_details = get_root_account_details(incident_dict)

        open_accounts_caseDetails = get_same_customer_accounts_from_case_details(customer_ref)
        open_accounts_incidents = get_same_customer_accounts_from_Incidents(customer_ref)

        logger_INC1A01.info(f"Found {len(open_accounts_caseDetails)} open case(s)")
        logger_INC1A01.info(f"Found {len(open_accounts_incidents)} open incident(s)")

        # Root Account
        entry = {
            "Incident_Id": root_account_details.get("Incident_Id", ""),
            "Case_Id": "",
            "Account_Num": root_account_details.get("Account_Num", ""),
            "Account_Status": root_account_details.get("Incident_Status", ""),
            "OutstandingBalance": root_account_details.get("Arrears", 0)
        }
        customer_link_accounts_details.append(entry)

        # Open Incidents
        for item in open_accounts_incidents:
            customer_link_accounts_details.append({
                "Incident_Id": item.get("Incident_Id", ""),
                "Case_Id": "",
                "Account_Num": item.get("Account_Num", ""),
                "Account_Status": item.get("Incident_Status", ""),
                "OutstandingBalance": item.get("Arrears", 0)
            })

        # Open Case Details
        for item in open_accounts_caseDetails:
            customer_link_accounts_details.append({
                "Incident_Id": "",
                "Case_Id": item.get("case_id", ""),
                "Account_Num": item.get("Account_Num", ""),
                "Account_Status": item.get("case_current_status", ""),
                "OutstandingBalance": item.get("bss_arrears_amount", 0)
            })

        incident_dict["Account_Cross_Details"] = customer_link_accounts_details
        logger_INC1A01.info(f"Linked accounts added to incident dictionary successfully: {incident_dict}")
        return incident_dict

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error during linking accounts: {e}")
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = str(e)
        return incident_dict

# endregion

# region MongoDB - Get Open Case Details
def get_same_customer_accounts_from_case_details(customer_ref: str) -> list:
    """
    Retrieves open case details from 'Case_details' collection for the given customer reference.

    Args:
        customer_ref (str): Customer reference ID

    Returns:
        list: List of dictionaries containing case details.
    """
    logger_INC1A01.info(f"Fetching open cases for customer_ref: {customer_ref}")
    client = None
    try:
        db_config = get_config("database", "DATABASE")
        client = MongoClient(db_config.get("mongo_uri").strip())
        client.admin.command('ping')
        db = client[db_config.get("db_name").strip()]

        collection = db["Case_details"]

        query = {
            "$expr": {
                "$and": [
                    {"$eq": ["$customer_ref", customer_ref]},
                    {
                        "$not": {
                            "$in": [
                                {"$toLower": "$case_current_status"},
                                ["close", "write-off", "abandoned", "withdraw"]
                            ]
                        }
                    }
                ]
            }
        }

        results = list(collection.find(query, {
            "case_id": 1,
            "Account_Num": 1,
            "case_current_status": 1,
            "bss_arrears_amount": 1,
            "_id": 0
        }))
        logger_INC1A01.debug(f"Query returned {results} case records.")
        return results

    except Exception as e:
        logger_INC1A01.error(f"Error retrieving case details for customer_ref {customer_ref}: {e}")
        return []

    finally:
        if client:
            client.close()
# endregion


# region MongoDB - Get Open Incident Details
def get_same_customer_accounts_from_Incidents(customer_ref: str) -> list:
    """
    Retrieves open incident records from 'Incidents' collection for the given customer reference.

    Args:
        customer_ref (str): Customer reference ID

    Returns:
        list: List of dictionaries containing incident details.
    """
    logger_INC1A01.info(f"Fetching open incidents for customer_ref: {customer_ref}")
    client = None
    try:
        db_config = get_config("database", "DATABASE")
        client = MongoClient(db_config.get("mongo_uri").strip())
        client.admin.command('ping')
        db = client[db_config.get("db_name").strip()]

        collection = db["Incidents"]

        query = {
            "$and": [
                { "customer_ref": customer_ref },
                {
                    "$or": [
                        { "Incident_Forwarded_By": { "$in": [None, ""] } },
                        { "Incident_Forwarded_By": { "$exists": False } }
                    ]
                },
                {
                    "$or": [
                        { "Incident_Forwarded_On": { "$in": [None, ""] } },
                        { "Incident_Forwarded_On": { "$exists": False } }
                    ]
                }
            ]
        }

        results = list(collection.find(query, {
            "Incident_Id": 1,
            "Account_Num": 1,
            "Incident_Status": 1,
            "Arrears": 1,
            "_id": 0
        }))
        logger_INC1A01.debug(f"Query returned {results} incident records.")
        return results

    except Exception as e:
        logger_INC1A01.error(f"Error retrieving incident details for customer_ref {customer_ref}: {e}")
        return []

    finally:
        if client:
            client.close()

# endregion


# region  Get Root Incident Details
def get_root_account_details(incident_dict):
    """
    Extracts root-level incident details from the given incident dictionary.

    Returns:
        dict: A dictionary with keys 'Incident_Id', 'Account_Num', 'Incident_Status', and 'Arrears'.
    """
    results = {
        "Incident_Id": incident_dict.get("Incident_Id"),
        "Account_Num": incident_dict.get("Account_Num"),
        "Incident_Status": incident_dict.get("Incident_Status"),
        "Arrears": incident_dict.get("Arrears")
    }

    logger_INC1A01.debug(f"root account details: {results}")

    return results
# endregion


if __name__ == '__main__':
    initialize_hash_maps()
    print(link_accounts_from_open_accounts(incident_dict))

