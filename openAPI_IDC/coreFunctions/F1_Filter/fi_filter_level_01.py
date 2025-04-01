from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from openAPI_IDC.coreFunctions.F1_Filter.f1_filter_logic import incident_filter_customer_type, \
    incident_filter_credit_class, \
    incident_filter_main_product_status, incident_filter_customer_segment, incident_filter_specific_customer_name, \
    incident_filter_specific_product_status
from openAPI_IDC.coreFunctions.config_manager import initialize_hash_maps
from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')


def do_f1_filter_level_01_for_incident_dict(value, incident_dict):

    match value:
        case 11:
            return incident_filter_credit_class(incident_dict)
        case 12:
            return incident_filter_customer_type(incident_dict)
        case 13:
            return incident_filter_main_product_status(incident_dict)
        case 14:
            return incident_filter_customer_segment(incident_dict)
        case 15:
            return incident_filter_specific_customer_name(incident_dict)
        case 16:
            return incident_filter_specific_product_status(incident_dict)
        case _:
            logger_INC1A01.error(f"No filter found for value: {value}")
            return incident_dict


if __name__ == "__main__":
    initialize_hash_maps()
    do_f1_filter_level_01_for_incident_dict(11, incident_dict)

