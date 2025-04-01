from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from openAPI_IDC.coreFunctions.config_manager import initialize_hash_maps
from utils.customerExceptions.cust_exceptions import DatabaseConnectionError, DataNotFoundError
from utils.database.connectDB import get_db_connection
from utils.logger.loggers import get_logger
# Get the logger
logger_INC1A01 = get_logger('INC1A01')

def insert_arrears_band(incident_dict):
    try:
         arrears_bands = get_arrears_bands_details()

         #example of return on arrears_bands
         #arrears_bands = {'AB-5_10': '5000-10000', 'AB-10_25': '10000-25000', 'AB-25_50': '25000-50000', 'AB-50_100': '50000-100000', 'AB-100<': '100000<', 'CP_Collect': 'CP_Collect'}

         if not arrears_bands:
             raise DataNotFoundError("Arrears Band not found")

         arrears = incident_dict.get("Arrears", 0)

         if arrears < 0 or not arrears:
             raise DataNotFoundError("Arrears value cannot be find")

         band_found = None

         for band, range_str in arrears_bands.items():
             if "<" in range_str:
                 # handle open-ended case like "100000<"
                 lower = float(range_str.replace("<", ""))
                 if arrears >= lower:
                     band_found = band
                     break
             elif "-" in range_str:
                 lower_str, upper_str = range_str.split("-")
                 lower = float(lower_str)
                 upper = float(upper_str)
                 if lower <= arrears < upper:
                     band_found = band
                     break
             elif range_str == "CP_Collect":
                 continue  # skip non-numeric band

         incident_dict["arrears_band"] = band_found
         return incident_dict

    except DataNotFoundError:
        logger_INC1A01.warning("Arrears Band not found")
        return {}

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error: {e}")
        return {}

def get_arrears_bands_details():
    db = False
    try:
        db = get_db_connection()

        if db is False:
            raise DatabaseConnectionError("Could not connect to MongoDB")

        collection = db["Arrears_bands"]

        # Fetch one document from the collection
        document = collection.find_one()

        if document:
            document.pop('_id', None)  # Remove _id if not needed
            arrears_bands = document   # All remaining key-values are dynamic fields
            logger_INC1A01.debug(f"Arrears Bands Data: {arrears_bands}")
            return arrears_bands
        else:
            logger_INC1A01.info("No arrears band data found in the collection.")

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error in get_arrears_bands_details: {e}")

    finally:
        if db is not False:
            db.client.close()

if __name__ == '__main__':
    initialize_hash_maps()
    x=insert_arrears_band(incident_dict)
    print(x)
