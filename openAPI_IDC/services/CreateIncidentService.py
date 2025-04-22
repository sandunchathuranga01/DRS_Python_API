"""
    Purpose:
    This module defines the service responsible for creating a new incident
    in the MongoDB database and applying necessary filters.

    Description:
    - Retrieves configuration from hash maps.
    - Applies filtering logic via `get_modified_incident_dict()`.
    - Ensures unique `Incident_Id` during insertion.
    - Handles DuplicateKeyError, filter rule rejection, and generic errors.
    - Returns a structured service response using the `IncidentServiceResponse` class.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from datetime import datetime
from pymongo.errors import DuplicateKeyError
from openAPI_IDC.coreFunctions.ConfigManager import get_config, initialize_hash_maps
from openAPI_IDC.coreFunctions.ModifyIncidentDict import get_modified_incident_dict
from openAPI_IDC.models.CreateIncidentModel import Incident
from pymongo import MongoClient
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
        self.success = success    # True if operation was successful
        self.data = data          # Holds the result (like incident_id)
        self.error = error        # Holds the exception, if any
# endregion

# region Main Function
def create_incident(incident: Incident):
    """
    Handles creation of a new incident document in the MongoDB database.
    Applies filters and ensures data integrity with unique index enforcement.
    Returns a structured service response.
    """
    client = None
    try:
        # Fetch MongoDB connection configuration from the configuration hash map
        db_config = get_config("database", "DATABASE")

        # Create a MongoDB client using the URI from the config
        client = MongoClient(db_config.get("mongo_uri").strip())

        # This ensures MongoDB is reachable and credentials are valid
        client.admin.command('ping')

        # Select the target database using its name from the config
        db = client[db_config.get("db_name").strip()]

    except Exception as e:
        # Handle any errors that occur during connection setup
        logger_INC1A01.info(f"Connection error: {e}")
        return IncidentServiceResponse(success=False, error="Mongo DB connection error")

    else:
        try:
            # Convert Pydantic model to dict and add status
            # CreateIncidentModel
            incident_dict = incident.dict()

            incident_dict["Incident_Status"] = "Success"

            # Apply filters/modifications to the incident (F1 filter)
            new_incident = get_modified_incident_dict(incident_dict)

            # If modification failed, raise a known exception
            if new_incident.get("Incident_Status") == "Error":
                raise NotModifiedResponse(new_incident.get("Status_Description"))

            # Access collection and ensure unique Incident_Id index
            collection = db["Incidents"]
            collection.create_index("Incident_Id", unique=True)

            incident_dict["updatedAt"] = datetime.now()

            # Insert the incident document
            collection.insert_one(new_incident)

            # Return successful response with the incident ID
            return IncidentServiceResponse(success=True, data=incident_dict["Incident_Id"])

        except DuplicateKeyError as dup_err:
            # Handle duplicate Incident_Id error
            logger_INC1A01.error(f"Duplicate Incident_Id: {incident_dict['Incident_Id']}")
            logger_INC1A01.error(f"Original incident: {incident}")
            return IncidentServiceResponse(success=False, error=dup_err)

        except NotModifiedResponse as mod_err:
            # Handle business rule failure
            logger_INC1A01.error(f"Incident dict modification failed: {mod_err}")
            logger_INC1A01.error(f"Original incident: {incident}")
            return IncidentServiceResponse(success=False, error=mod_err)

        except Exception as e:
            # Handle any other exception
            logger_INC1A01.error(f"Error inserting incident: {e}")
            logger_INC1A01.error(f"Original incident: {incident}")
            return IncidentServiceResponse(success=False, error=e)

    finally:
        # Close the MongoDB client connection
        if client:
            client.close()
            logger_INC1A01.info("MongoDB connection closed.")
# endregion
