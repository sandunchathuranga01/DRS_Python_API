from datetime import datetime


from pymongo.errors import DuplicateKeyError
from openAPI_IDC.coreFunctions.config_manager import initialize_hash_maps
from openAPI_IDC.coreFunctions.modifyIncidentDict import get_f1_filter_result
from openAPI_IDC.models.CreateIncidentModel import Incident
from utils.customerExceptions.cust_exceptions import NotModifiedResponse
from utils.database.connectDB import get_db_connection
from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')

initialize_hash_maps()


def create_incident(incident: Incident):
    db = False
    try:
        # Get database connection
        db = get_db_connection()

        if db is False:
            raise Exception("Database connection failed")

        # Get the actual collection
        collection = db["Incidents"]

        # Ensure index on Incident_Id
        collection.create_index("Incident_Id", unique=True)

        incident_dict = incident.dict()

        incident_dict['updatedAt'] = datetime.now()

        logger_INC1A01.debug(incident_dict)

        New_Incident_dict = get_f1_filter_result(incident_dict)

        if not New_Incident_dict :
            raise NotModifiedResponse("failed to modify incident dict")

        collection.insert_one(New_Incident_dict)

        return int(incident_dict["Incident_Id"])

    except DuplicateKeyError:
        logger_INC1A01.warning(f"Duplicate Incident_Id: {incident_dict['Incident_Id']} - Incident already exists.")
        return -2

    except NotModifiedResponse:
        logger_INC1A01.warning("failed to modify incident dict")
        return -3

    except Exception as e:
        logger_INC1A01.error(f"Error inserting incident {e}")
        return -1

    finally:
        if db is not False:
            db.client.close()