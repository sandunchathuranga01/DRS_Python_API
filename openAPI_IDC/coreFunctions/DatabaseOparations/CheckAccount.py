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
from openAPI_IDC.coreFunctions.ConfigManager import get_config
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region has_open_case_for_account
def has_open_case_for_account(incident_dict):
    """
    Checks if there is any open case (status not 'close') for the given Account_Num.
    Returns True if at least one open case is found, otherwise False.
    """
    client = None
    try:
        # Load MongoDB configuration from the config hash map
        db_config = get_config("database", "DATABASE")

        # Create the MongoDB client
        client = MongoClient(db_config.get("mongo_uri").strip())

        # Check the database connection (ping)
        client.admin.command('ping')

        # Select the target database
        db = client[db_config.get("db_name").strip()]

    except Exception as e:
        # Log and return if connection fails
        logger_INC1A01.error(f"Connection error: {e}")
        return False

    else:
        try:
            # Access the case collection
            collection = db["Case_details"]

            # Get the account number from incident_dict
            account_number = incident_dict.get("Account_Num")

            if not account_number:
                logger_INC1A01.warning("Account number not found in incident_dict.")
                return False

            # Find cases with the same account number
            case_documents = collection.find({"Account_Num": account_number})

            # Check if any case is not closed
            for case in case_documents:
                status = case.get("case_current_status", "").lower()
                if status != "close":
                    logger_INC1A01.info(f"Open case found for Account_Num: {account_number}, status: {status}")
                    return True

            # No open cases found
            logger_INC1A01.info(f"No open cases found for Account_Num: {account_number}")
            return False

        except Exception as e:
            logger_INC1A01.error(f"Error while checking open cases for account: {e}")
            return False

    finally:
        # Always close the MongoDB connection
        if client:
            client.close()
            logger_INC1A01.info("MongoDB connection closed.")
# endregion

# region link_accounts_from_open_cases
def link_accounts_from_open_cases(incident_dict):
    """
    Finds all open cases for the same customer_ref and adds related Account_Num
    to the 'Link_Accounts' field of the incident_dict (if not already added).
    Returns the updated incident_dict.
    """
    client = None
    try:
        # Load MongoDB configuration
        db_config = get_config("database", "DATABASE")

        # Create MongoDB client and connect
        client = MongoClient(db_config.get("mongo_uri").strip())
        client.admin.command('ping')
        db = client[db_config.get("db_name").strip()]

    except Exception as e:
        logger_INC1A01.error(f"Connection error: {e}")
        return False

    else:
        try:
            # Get the case collection
            collection = db["Case_details"]

            # Get customer_ref from incident
            customer_ref = incident_dict.get("Customer_Details", {}).get("customer_ref")

            if not customer_ref:
                logger_INC1A01.warning("customer_ref not found in incident_dict.")
                return incident_dict

            # Make sure Link_Accounts is a list
            if not isinstance(incident_dict.get("Link_Accounts"), list):
                incident_dict["Link_Accounts"] = []

            # Search for all open cases with the same customer_ref
            case_documents = collection.find({"customer_ref": customer_ref})
            added_any = False  # Track if new accounts were added

            for case in case_documents:
                status = case.get("case_current_status", "").lower()

                # Only process cases that are not closed
                if status != "close":
                    account_num = case.get("Account_Num")
                    if account_num:
                        # Check if this account is already linked
                        already_linked = any(
                            acc.get("Account_Num") == account_num
                            for acc in incident_dict["Link_Accounts"]
                        )
                        if not already_linked:
                            # Add new linked account
                            incident_dict["Link_Accounts"].append({"Account_Num": account_num})
                            logger_INC1A01.info(f"Linked Account_Num {account_num} added.")
                            added_any = True
                        else:
                            logger_INC1A01.info(f"Account_Num {account_num} already exists in Link_Accounts.")

            if not added_any:
                logger_INC1A01.info(f"No new open accounts found for customer_ref: {customer_ref}")

            return incident_dict

        except Exception as e:
            logger_INC1A01.error(f"Error checking open cases for customer_ref: {e}")
            return incident_dict

    finally:
        # Always close the client
        if client:
            client.close()
            logger_INC1A01.info("MongoDB connection closed.")
# endregion
