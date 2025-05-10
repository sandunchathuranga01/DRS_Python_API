# region Imports
from pymongo import DESCENDING
from pymongo import MongoClient
from openAPI_IDC.coreFunctions.ConfigManager import get_config
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

def get_next_incident_id():
    """
    Retrieves the next available Incident_Id by querying the highest current value and incrementing it by 1.
    Returns:
        int: The next Incident_Id.
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
            # Access the 'Incidents' collection
            IncidentCollection = db["Incidents"]

            highest_doc = IncidentCollection.find_one(sort=[("Incident_Id", DESCENDING)])

            next_id = (highest_doc["Incident_Id"] + 1) if highest_doc and "Incident_Id" in highest_doc else 1

            logger_INC1A01.info(f"Incident_Id not provided. Found new Incident_Id: {next_id}")

            return next_id

        except Exception as e:
            # Log query errors
            logger_INC1A01.error(f"Unexpected error in get_arrears_bands_details: {e}")
            return -1

    finally:
        # Clean up MongoDB connection
        if client:
            client.close()