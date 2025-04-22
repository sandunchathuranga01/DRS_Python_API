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
from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region has_open_case_for_account
def has_open_case_for_account(incident_dict):
    """
    Checks if there is any open case (status not in ["Case Close", "Write-Off", "Abandoned", "Withdraw"])
    for the given Account_Num in the incident_dict.

    Returns:
        True  -> if at least one open case is found
        False -> if no open cases or if an error occurs
    """
    client = None
    try:
        # Load MongoDB configuration (URI and DB name) from the config hash map
        db_config = get_config("database", "DATABASE")

        # Initialize the MongoDB client
        client = MongoClient(db_config.get("mongo_uri").strip())

        # Test the connection to the MongoDB server
        client.admin.command('ping')

        # Access the specific database
        db = client[db_config.get("db_name").strip()]

    except Exception as e:
        # Log any errors during connection setup
        logger_INC1A01.error(f"Connection error: {e}")

    else:
        try:
            # Select the Case_details collection
            collection = db["Case_details"]

            # Build query to find open cases for the given account number
            query = {
                "account_no": incident_dict.get("Account_Num"),
                "case_current_status": {
                    # Exclude cases that are already closed or inactive
                    "$nin": ["Case Close", "Write-Off", "Abandoned", "Withdraw"]
                }
            }

            # Count how many documents match the criteria
            Count_of_active_cases = len(list(collection.find(query)))

            # Return True if there's at least one open case
            if Count_of_active_cases > 0:
                return True
            else:
                return False

        except Exception as e:
            # Log any errors that occur during query execution
            logger_INC1A01.error(f"Error while checking open cases for account: {e}")
            return False

    finally:
        # Ensure the MongoDB client is closed after the operation
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


if __name__ == "__main__":
    has_open_case_for_account(incident_dict)
