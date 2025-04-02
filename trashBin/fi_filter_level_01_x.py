from openAPI_IDC.coreFunctions.F1_Filter.f1_filter_logic import incident_filter_customer_type, \
    incident_filter_credit_class, \
    incident_filter_main_product_status, incident_filter_customer_segment, incident_filter_specific_customer_name, \
    incident_filter_specific_product_status
from utils.customerExceptions.cust_exceptions import NotModifiedResponse
from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')

def do_f1_filter_level_01_for_incident_dict(value, incident_dict):
    match value:
        case 11:
            try:
                filter_01_results = incident_filter_credit_class(incident_dict)
                if not filter_01_results:
                    raise NotModifiedResponse ("failed to modify incident credit class")

                credit_class_id = filter_01_results.get("Account_Details", {}).get("Credit_Class_Id")
                if not credit_class_id:
                    return

            except NotModifiedResponse as not_modified_response:
                logger_INC1A01.info(f"Not modified response : {not_modified_response}")
            except Exception as e:
                logger_INC1A01.error(f"Unexpected error: {e}")
                return False


















        case 12:
            filter_02_results = incident_filter_customer_type(incident_dict)
            if filter_02_results is False:
                print(filter_02_results)
                return False
            return filter_02_results
        case 13:
            filter_03_results = incident_filter_main_product_status(incident_dict)
            if filter_03_results is False:
                print(filter_03_results)
                return False
            return filter_03_results
        case 14:
            filter_04_results = incident_filter_customer_segment(incident_dict)
            if filter_04_results is False:
                print(filter_04_results)
                return False
            return filter_04_results
        case 15:
            filter_05_results = incident_filter_specific_customer_name(incident_dict)
            if filter_05_results is False:
                print(filter_05_results)
                return False
            return filter_05_results
        case 16:
            filter_06_results = incident_filter_specific_product_status(incident_dict)
            if filter_06_results is False:
                print(filter_06_results)
                return False
            return filter_06_results
        case _:
            logger_INC1A01.warning(f"No filter found for value: {value}")
            print("No filter found")
            return False