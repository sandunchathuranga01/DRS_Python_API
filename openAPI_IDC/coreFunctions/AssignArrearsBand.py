"""
    Purpose:
    Assigns an arrears band to an incident based on its 'Arrears' value.

    Description:
    This function fetches dynamic band definitions from the database and
    matches the given arrears value to a defined band. It supports formats:
        - '<value'  → less than
        - 'value<'  → greater than or equal
        - 'x-y'     → range [x, y)

    Returns:
        The original incident_dict updated with 'arrears_band'.
        If an error occurs, the incident status is marked as 'Error'.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from datetime import datetime

from openAPI_IDC.coreFunctions.ConfigManager import initialize_hash_maps
from openAPI_IDC.coreFunctions.DatabaseOparations.GetArrearsBandDetails import get_arrears_bands_details
from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from utils.customerExceptions.cust_exceptions import DataNotFoundError
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Arrears Band Assignment Function
def assign_arrears_band(incident_dict):
    """
    Assigns an arrears band to the incident_dict based on its 'Arrears' value.

    The bands are retrieved from MongoDB and follow these formats:
        - '<1000' means arrears less than 1000
        - '1000-2500' means arrears between 1000 and 2500 (inclusive lower, exclusive upper)
        - '100000<' means arrears greater than or equal to 100000

    Args:
        incident_dict (dict): The incident data with an 'Arrears' field.

    Returns:
        dict: The updated incident_dict with 'arrears_band' field added.
              If an error occurs, a dict with "error" field is returned.
    """
    try:
        # Fetch arrears band configuration from DB
        arrears_bands = get_arrears_bands_details()

        # If no data found, raise a custom exception
        if not arrears_bands:
            raise DataNotFoundError("Arrears Band not found")

        # Get arrears value from the incident
        arrears = incident_dict.get("Arrears", 0)

        band_found = None  # Will hold the matched band name

        # Loop through each band and match the arrears value to the defined range
        for band, range_str in arrears_bands.items():
            if isinstance(range_str, str):
                range_str = range_str.strip()

                # Format: '100000<', meaning arrears >= 100000
                if range_str.endswith("<"):
                    lower = float(range_str.replace("<", ""))
                    if arrears >= lower:
                        band_found = band
                        break

                # Format: '<1000', meaning arrears < 1000
                elif range_str.startswith("<"):
                    upper = float(range_str.replace("<", ""))
                    if arrears < upper:
                        band_found = band
                        break

                # Format: '1000-2500', meaning arrears in range [1000, 2500)
                elif "-" in range_str:
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

        # Assign the matched band to the incident
        incident_dict["Arrears_Band"] = band_found
        logger_INC1A01.info(f"arrears band assign incident dict: {incident_dict}")
        return incident_dict

    except DataNotFoundError as Data_Not_Found_Error:
        # Log and return error if arrears band data is not available
        logger_INC1A01.warning(f"{Data_Not_Found_Error}")
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = Data_Not_Found_Error
        return incident_dict

    except Exception as e:
        # Catch-all for any unexpected errors
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict
# endregion


if __name__ == "__main__":
    initialize_hash_maps()
    assign_arrears_band(incident_dict)