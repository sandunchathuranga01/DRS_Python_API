from datetime import datetime, timezone


from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from utils.customerExceptions.cust_exceptions import DataNotFoundError
from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')

def do_f1_filter_level_02_for_incident_dict(incident_dict):
    try:
        arrears = incident_dict.get("Arrears")
        if not arrears:
            raise DataNotFoundError("arrears is empty")

        # now_iso = datetime.now(timezone.utc).isoformat()
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        if arrears > 5000:
            incident_dict["Incident_Status"] = "Open No agent"
            incident_dict["Status_Description"] = "Customer in high arrears – no agent assigned"
        elif 1000 < arrears <= 5000:
            incident_dict["Incident_Status"] = "Direct LOD"
            incident_dict["Status_Description"] = "Direct Letter of Demand issued"
        elif arrears <= 1000:
            incident_dict["Incident_Status"] = "Open CPE Collect"
            incident_dict["Status_Description"] = "Collection action by CPE team"
        else:
            incident_dict["Incident_Status"] = "Unknown"
            incident_dict["Status_Description"] = "Arrears amount invalid"

        incident_dict["Incident_Status_Dtm"] = {"$date": now_iso}

        # New_incident_dict = incident_dict
        logger_INC1A01.debug(incident_dict)
        return incident_dict

    except DataNotFoundError as data_not_found_error:
        logger_INC1A01.error(data_not_found_error)
        return {}

    except Exception as e:
        logger_INC1A01.error(e)
        return {}


if __name__ == '__main__':
    print(do_f1_filter_level_02_for_incident_dict(incident_dict))

