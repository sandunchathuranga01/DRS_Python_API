"""
    Purpose:
    This module is responsible for inserting a new incident into the MongoDB database
    and updating cross-account details in related incident and case records.

    Description:
    - Connects to MongoDB using configurations fetched from the hash map.
    - Inserts a new incident document while enforcing a unique index on `Incident_Id`.
    - If linked accounts exist, it updates corresponding records in both `Incidents` and `Case_details` collections
      by appending root account details to the `Account_Cross_Details` array.
    - Implements a rollback mechanism if any update fails after insertion.
    - Logs every step of the process for traceability and debugging.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from pymongo import MongoClient
from typing import List, Dict
from pymongo.errors import DuplicateKeyError
from openAPI_IDC.coreFunctions.ConfigManager import get_config
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Main Function
def create_incident_data_manipulation(customer_link_accounts_details: List[Dict], new_incident: Dict):
    """
        Inserts a new incident into the MongoDB 'Incidents' collection and, if applicable,
        updates related incidents and case documents with cross-account references.
    """
    client = None
    inserted_id = None

    # region MongoDB Connection
    try:
        db_config = get_config("database", "DATABASE")
        client = MongoClient(db_config.get("mongo_uri").strip())
        client.admin.command('ping')
        db = client[db_config.get("db_name").strip()]
    except Exception as e:
        logger_INC1A01.info(f"Connection error: {e}")
        return {"success": False, "error": "Mongo DB connection error"}
    # endregion

    else:
        # region Collections Initialization
        IncidentCollection = db["Incidents"]
        CaseCollection = db["Case_details"]
        # endregion

        try:
            # region Incident Insertion
            try:
                IncidentCollection.create_index("Incident_Id", unique=True)
                root_result = IncidentCollection.insert_one(new_incident)
                inserted_id = root_result.inserted_id
                logger_INC1A01.info(f"Incident inserted successfully with ID: {new_incident['Incident_Id']}")
            except DuplicateKeyError as dup_err:
                logger_INC1A01.error(f"Duplicate Incident_Id: {new_incident['Incident_Id']}")
                logger_INC1A01.error(f"Original incident: {new_incident}")
                return {"success": False, "error": dup_err}
            except Exception as e:
                logger_INC1A01.error(f"Insert error: {e}")
                return {"success": False, "error": f"Incident insertion failed {new_incident['Incident_Id']}"}
            # endregion

            # region Linked Account Checks
            if not customer_link_accounts_details or len(customer_link_accounts_details) < 2:
                logger_INC1A01.info("No linked accounts to process.")
                return {"success": True, "data": new_incident["Incident_Id"]}

            root_details = customer_link_accounts_details[0]
            linked_accounts = customer_link_accounts_details[1:]
            incident_id_list = [item['Incident_Id'] for item in linked_accounts if 'Incident_Id' in item and item['Incident_Id'] != '']
            case_id_list = [item['Case_Id'] for item in linked_accounts if item.get('Case_Id')]

            cross_detail_entry = {
                "Incident_Id": root_details.get("Incident_Id"),
                "Case_Id": root_details.get("Case_Id"),
                "Account_Num": root_details.get("Account_Num"),
                "Account_Status": root_details.get("Account_Status"),
                "OutstandingBalance": root_details.get("OutstandingBalance"),
            }
            # endregion

            # region Update Linked Incidents
            for incident_id in incident_id_list:
                result = IncidentCollection.update_one(
                    {"Incident_Id": incident_id},
                    {"$addToSet": {"Account_Cross_Details": cross_detail_entry}}
                )
                if result.matched_count == 0:
                    raise Exception(f"Incident {new_incident['Incident_Id']} not found during update")
            # endregion

            # region Update Linked Cases
            for case_id in case_id_list:
                result = CaseCollection.update_one(
                    {"case_id": case_id},
                    {"$addToSet": {"Account_Cross_Details": cross_detail_entry}}
                )
                if result.matched_count == 0:
                    raise Exception(f"Case {case_id} not found during update")
            # endregion

            logger_INC1A01.info("All updates completed successfully.")
            return {"success": True, "data": new_incident["Incident_Id"]}

        # region Rollback on Failure
        except Exception as e:
            logger_INC1A01.error(f"Manual rollback triggered due to: {e}")
            if inserted_id:
                try:
                    IncidentCollection.delete_one({"_id": inserted_id})
                    logger_INC1A01.warning(f"Rolled back inserted incident with ID: {inserted_id}")
                except Exception as rollback_error:
                    logger_INC1A01.error(f"Rollback failed: {rollback_error}")
            return {"success": False, "error": str(e)}
        # endregion

    # region Close Connection
    finally:
        if client:
            client.close()
    # endregion
# endregion
