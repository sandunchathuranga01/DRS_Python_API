from datetime import datetime

from openAPI_IDC.coreFunctions.F1_Filter.fi_filter_level_01 import do_f1_filter_level_01_for_incident_dict
from openAPI_IDC.coreFunctions.Insert_arrears_band import insert_arrears_band
from openAPI_IDC.coreFunctions.config_manager import initialize_hash_maps
from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from openAPI_IDC.coreFunctions.F1_Filter.f1_filter_level_02 import do_f1_filter_level_02_for_incident_dict
from openAPI_IDC.coreFunctions.F1_Filter.get_f1_filter_details import get_new_filter_id_list_from_active_filters
from utils.customerExceptions.cust_exceptions import DataNotFoundError, NotModifiedResponse

from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')

def do_f1_filter_for_incident_dict(incident_dict):
    try:
        # Use predefined filter_ids
        filter_ids = get_new_filter_id_list_from_active_filters()

        if not filter_ids:
            raise DataNotFoundError("Missing Filter IDs")

        for filter_id in filter_ids:
            logger_INC1A01.debug(f"Running filter ID: {filter_id}")
            updated_incident = do_f1_filter_level_01_for_incident_dict(filter_id, incident_dict)

            if isinstance(updated_incident, dict):
                # Check if filter was triggered (i.e., 'Filtered_Reason' is present)
                if updated_incident.get("Filtered_Reason"):
                    updated_incident["Incident_Status"] = "reject pending"
                    updated_incident["Incident_Status_Dtm"] = datetime.now().isoformat()

                    logger_INC1A01.info(
                        f"Incident rejected at filter_id {filter_id} with reason: {updated_incident['Filtered_Reason']}")
                    return updated_incident
                else:
                    # No filter triggered; continue to next
                    continue

        # If all filters returned False, execute f1 L2 filtering
        if incident_dict.get("Filtered_Reason"):
            logger_INC1A01.info("Incident already filtered by level 1 filter. Skipping level 2 filtering.")
            return incident_dict

            # Run level 2 filtering if no level 1 filter triggered
        logger_INC1A01.info("No filter triggered at level 01. Running level 02 filters...")
        Filtered_incident_dict = do_f1_filter_level_02_for_incident_dict(incident_dict)

        return Filtered_incident_dict

    except DataNotFoundError as no_valid_data_error:
        logger_INC1A01.info(f"No valid data found.{no_valid_data_error}")
        return {}

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error: {e}", exc_info=True)
        return {}

def get_f1_filter_result(incident_dict):
    initialize_hash_maps()
    try:
        if isinstance(incident_dict, dict):  # Ensure it's properly initialized

            #insert arrears band into coming incident dictionary
            arrears_band_inserted_incident_dict = insert_arrears_band(incident_dict)
            logger_INC1A01.debug(arrears_band_inserted_incident_dict)

            #checking arrears_band_inserted_incident_dict availability
            if not arrears_band_inserted_incident_dict:
                raise NotModifiedResponse("Arrears band not modified")

            #assign arrears_band_inserted_incident_dict in to f1 filter
            final_result = do_f1_filter_for_incident_dict(arrears_band_inserted_incident_dict)
            logger_INC1A01.debug("Final result:", final_result)

            #return final result
            return final_result

        else:
            logger_INC1A01.error("incident_dict is not a valid dictionary!")
            return {}

    except NotModifiedResponse:
        logger_INC1A01.info("No valid data found,Arrears band not modified")
        return {}

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error: {e}")
        return {}

#want to remove when use this function in createIncidentService
def main():
    try:
        New_Incident_dict = get_f1_filter_result(incident_dict)

        if not New_Incident_dict:
            return NotModifiedResponse("failed to modify incident dict")

        print(New_Incident_dict)

    except NotModifiedResponse:
        logger_INC1A01.error("failed to modify incident dict")

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()