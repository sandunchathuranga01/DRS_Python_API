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

        if not arrears_bands:
            raise DataNotFoundError("Arrears Band not found")

        arrears = incident_dict.get("Arrears", 0)

        # if arrears is None or not isinstance(arrears, (int, float)) or arrears < 0:
        #     raise DataNotFoundError("Invalid or missing arrears value")

        band_found = None

        for band, range_str in arrears_bands.items():
            if isinstance(range_str, str):
                range_str = range_str.strip()

                if range_str.endswith("<"):  # e.g. '100000<'
                    lower = float(range_str.replace("<", ""))
                    if arrears >= lower:
                        band_found = band
                        break

                elif range_str.startswith("<"):  # e.g. '<1000'
                    upper = float(range_str.replace("<", ""))
                    if arrears < upper:
                        band_found = band
                        break

                elif "-" in range_str:  # e.g. '1000-2500'
                    try:
                        lower_str, upper_str = range_str.split("-")
                        lower = float(lower_str)
                        upper = float(upper_str)
                        if lower <= arrears < upper:
                            band_found = band
                            break
                    except ValueError:
                        logger_INC1A01.warning(f"Invalid range format in band: {range_str}")
                        continue

        incident_dict["arrears_band"] = band_found
        return incident_dict

    except DataNotFoundError as e:
        logger_INC1A01.warning(f"{e}")
        return {}

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error: {e}")
        return {}


def get_arrears_bands_details():
    db = False
    try:
        db = get_db_connection()

        if db is False:
            raise DatabaseConnectionError("Could not connect to Data base")

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
            return {}

    except DatabaseConnectionError:
        logger_INC1A01.error("Could not connect to Data base")
        return {}

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error in get_arrears_bands_details: {e}")
        return {}

    finally:
        if db is not False:
            db.client.close()

if __name__ == '__main__':
    initialize_hash_maps()
    x=insert_arrears_band(incident_dict)
    print(x)
