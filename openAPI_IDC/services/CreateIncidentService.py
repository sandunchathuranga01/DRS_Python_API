"""
    Purpose:
    This module provides a service function to create a new incident in the MongoDB database.
    It ensures the incident data is modified using predefined filters, validates unique incident IDs,
    and manages account linking before insertion.

    Description:
    - Initializes configuration using `initialize_hash_maps`.
    - Converts the Pydantic `Incident` model to a dictionary and adds metadata.
    - Applies data transformation and filtering using `get_modified_incident_dict`.
    - Validates and links related customer accounts before inserting the incident.
    - Handles all exceptions and logs relevant errors.
    - Returns a structured response via `IncidentServiceResponse`.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from datetime import datetime
from openAPI_IDC.coreFunctions.ConfigManager import initialize_hash_maps
from openAPI_IDC.coreFunctions.DataManipulation import create_incident_data_manipulation
from openAPI_IDC.coreFunctions.DatabaseOparations.CheckAccount import customer_link_accounts_details
from openAPI_IDC.coreFunctions.DatabaseOparations.getNextIncidentID import get_next_incident_id
from openAPI_IDC.coreFunctions.ModifyIncidentDict import get_modified_incident_dict
from openAPI_IDC.models.CreateIncidentModel import Incident
from utils.customerExceptions.cust_exceptions import NotModifiedResponse
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Initialize Configuration
initialize_hash_maps()
# endregion

# region Response Class
class IncidentServiceResponse:
    def __init__(self, success: bool, data=None, error: Exception = None):
        self.success = success
        self.data = data
        self.error = error
# endregion


# region Main Function
def create_incident(incident: Incident):
    """
    Creates and inserts a new incident document into the MongoDB database.

    Steps:
    - Converts input Pydantic model to a dictionary and marks status as 'Success'.
    - Applies filtering and modification logic to the incident data.
    - Checks for modification rejection via filter rules.
    - Links customer-related account details before insertion.
    - Calls data manipulation logic to perform MongoDB insertion.
    - Returns a structured success or error response.

    Args:
        incident (Incident): A validated Pydantic model representing the new incident.

    Returns:
        IncidentServiceResponse: Contains success flag, inserted incident ID, or an error.
    """
    try:
        logger_INC1A01.debug(incident)
        # Convert the Pydantic model(CreateIncidentModel.py) to a dictionary
        incident_dict = incident.dict()


        # Initial status set to Success
        incident_dict["Incident_Status"] = "Success"

        # Placeholder for filter reason (empty initially)
        incident_dict["Filtered_Reason"] = ""

        # Handle Incident_Id
        if incident_dict.get("Incident_Id"):
            # If Incident_Id is already provided in the request, log it
            logger_INC1A01.info(f"Incident_Id provided: {incident_dict['Incident_Id']}")
        else:
            # Log that no Incident_Id was provided and a new one will be generated
            logger_INC1A01.info("Incident_Id not provided. Finding new Incident_Id for incident...")

            # Call helper function to get the next available Incident_Id
            next_id = get_next_incident_id()

            # If ID generation failed (returns -1), log and raise an exception
            if next_id < 0:
                logger_INC1A01.error("New Incident_Id creation error")
                raise NotModifiedResponse("New Incident_Id creation error")

            # Assign the newly generated Incident_Id to the dictionary
            incident_dict["Incident_Id"] = next_id

            # Log the newly generated Incident_Id
            logger_INC1A01.info(f"New Incident_Id assigned: {incident_dict['Incident_Id']}")

        # Log the final Incident_Id that will be used (whether provided or newly generated)
        logger_INC1A01.debug(f"Final Incident_Id being used: {incident_dict['Incident_Id']}")

        logger_INC1A01.debug(incident_dict)

        # Modify retrieve incident data
        new_incident = get_modified_incident_dict(incident_dict)
        #new_incident = incident_dict

        logger_INC1A01.debug(new_incident)

        # Stop if filtering rejected the incident
        if new_incident.get("Incident_Status") == "Error":
            raise NotModifiedResponse(new_incident.get("Status_Description"))

        # Add/update current timestamp
        incident_dict["updatedAt"] = datetime.now()

        # Insert and update incident
        result = create_incident_data_manipulation(customer_link_accounts_details, new_incident)

        logger_INC1A01.debug(result)

        if not result.get("success"):
            logger_INC1A01.error(f"Data manipulation failed: {result.get('error')}")
            return IncidentServiceResponse(success=False, error=result.get("error"))

        return IncidentServiceResponse(success=True, data=result.get("data"))

    except NotModifiedResponse as mod_err:
        logger_INC1A01.error(f"Incident dict modification failed: {mod_err}")
        logger_INC1A01.error(f"Original incident: {incident}")
        return IncidentServiceResponse(success=False, error=mod_err)

    except Exception as e:
        logger_INC1A01.error(f"Error inserting incident: {e}")
        logger_INC1A01.error(f"Original incident: {incident}")
        return IncidentServiceResponse(success=False, error=e)

# endregion
