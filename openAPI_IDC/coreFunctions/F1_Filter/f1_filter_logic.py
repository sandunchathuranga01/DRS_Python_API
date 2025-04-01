from datetime import datetime
import re

from openAPI_IDC.coreFunctions.config_manager import initialize_hash_maps
from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from openAPI_IDC.coreFunctions.F1_Filter.get_f1_filter_details import get_line_by_new_filter_id
from utils.customerExceptions.cust_exceptions import  DataNotFoundError
from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')

#filter_11_details
def incident_filter_credit_class(incident_dict):
    try:
        logger_INC1A01.debug("Starting incident filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        #filter_01_details = {'new_filter_id': 11, 'filter_rule': 'BSS Credit Class', 'operator': 'equal', 'rule_values': [3, 7, 10, 43],'source_type':['Special', 'Pilot_suspend']}
        filter_01_details = get_line_by_new_filter_id(11)

        logger_INC1A01.debug("Filter details: %s", filter_01_details)

        # Extract the Credit_Class_Id from Account_Details
        credit_class_id = incident_dict.get("Account_Details", {}).get("Credit_Class_Id")
        if not credit_class_id:
            raise DataNotFoundError("No credit class id")

        logger_INC1A01.debug("Extracted Credit_Class_Id: %s", credit_class_id)

        # Check if the Credit_Class_Id is in the rule values using the 'equal' operator.
        if filter_01_details["operator"] == "equal" and credit_class_id in filter_01_details["rule_values"]:
            logger_INC1A01.info("Incident meets filter condition for Credit_Class_Id: %s", credit_class_id)

            # Update Filtered_Reason with the filter rule and operator
            incident_dict["Filtered_Reason"] = f"Incident meets filter condition for {filter_01_details['filter_rule']},Credit_Class_Id: {credit_class_id}"
            logger_INC1A01.debug("Filtered_Reason updated to: %s", incident_dict["Filtered_Reason"])

            logger_INC1A01.debug(incident_dict)
            return incident_dict #New incident dict
        else:
            logger_INC1A01.debug("Incident does not meet filter condition; no updates applied.")
            return incident_dict #Not filter incident dict

    except DataNotFoundError:
        logger_INC1A01.debug("No credit class id found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict  # Not filter incident dict

    except Exception as e:
        logger_INC1A01.exception("An unexpected error occurred: %s", e)
        return incident_dict

# filter_12_details
def incident_filter_customer_type(incident_dict):

    try:
        logger_INC1A01.debug("Starting customer type filter for Incident_Id: %s",incident_dict.get("Incident_Id", "Unknown"))

        # Define filter details for Customer Type Name
        #filter_03_details = {'filter_rule': 'Customer Type Name','operator': 'equal','rule_values': ['SLT']}

        filter_02_details = get_line_by_new_filter_id(12)
        logger_INC1A01.debug("Filter details: %s", filter_02_details)

        # Extract Customer_Type_Name from Customer_Details
        customer_type_name = incident_dict.get("Customer_Details", {}).get("Customer_Type_Name")
        if not customer_type_name:
            raise DataNotFoundError("No customer type name")

        logger_INC1A01.debug("Extracted Customer_Type_Name: %s", customer_type_name)



        # Check if the filter condition is met (operator 'equal')
        if filter_02_details["operator"] == "equal" and customer_type_name in filter_02_details["rule_values"]:
            logger_INC1A01.info("Customer type filter triggered for Customer_Type_Name: %s", customer_type_name)

            # Update incident details based on filter trigger
            incident_dict["Filtered_Reason"] = f"{filter_02_details['filter_rule']},customer_type_name: {customer_type_name}"
            logger_INC1A01.info("Incident rejected due to customer type filter")

            logger_INC1A01.debug(incident_dict)
            return incident_dict #New incident dict

        else:
            logger_INC1A01.debug("Customer type filter not triggered for Incident_Id: %s",incident_dict.get("Incident_Id"))
            return incident_dict #Not filter incident dict

    except DataNotFoundError:
        logger_INC1A01.debug("No customer type name found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict  # Not filter incident dict

    except Exception as e:
        logger_INC1A01.exception("An error occurred while applying customer type filter: %s", e)
        return incident_dict

# filter_13_details
def incident_filter_main_product_status(incident_dict):
    try:
        logger_INC1A01.debug("Starting main product status filter for Incident_Id: %s",incident_dict.get("Incident_Id", "Unknown"))

        filter_03_details = get_line_by_new_filter_id(13)
        logger_INC1A01.debug("Filter details: %s", filter_03_details)

        product_details = incident_dict.get("Product_Details", [])
        if not product_details:
            raise DataNotFoundError("No product details")

        logger_INC1A01.debug("Extracted Product_Details: %s", product_details)


        # Check if any product status matches the rule values
        match_found = any(
            product.get("Product_Status") in filter_03_details["rule_values"]
            for product in product_details
        )

        if filter_03_details["operator"] == "equal" and match_found:
            logger_INC1A01.info("Main product status filter triggered. At least one product has a matching status.")

            matching_statuses = [p["Product_Status"] for p in product_details if p.get("Product_Status") in filter_03_details["rule_values"]]
            incident_dict["Filtered_Reason"] = f"{filter_03_details['filter_rule']}, matched_statuses: {matching_statuses}"


            logger_INC1A01.info("Incident rejected due to main product status filter.")
            logger_INC1A01.debug("Updated incident: %s", incident_dict)
            return incident_dict

        else:
            logger_INC1A01.debug("Main product status filter not triggered for Incident_Id: %s",incident_dict.get("Incident_Id", "Unknown"))
            return incident_dict

    except DataNotFoundError:
        logger_INC1A01.info("No product details found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.error("unexpected error occurred: %s", e)
        return incident_dict


# filter_14_details
def incident_filter_customer_segment(incident_dict):
    try:
        logger_INC1A01.debug("Starting customer segment filter for Incident_Id: %s",
                     incident_dict.get("Incident_Id", "Unknown"))

        # Define filter details for Customer Segment
        # filter_05_details = {'filter_rule': 'Customer Segment','operator': 'equal','rule_values': [2, 4, 6, 7]}

        filter_04_details = get_line_by_new_filter_id(14)
        logger_INC1A01.debug("Filter details: %s", filter_04_details)

        # Extract Customer_Segment from Account_Details
        customer_segment = incident_dict.get("Account_Details", {}).get("Customer_Segment")
        if not customer_segment:
            raise DataNotFoundError("No customer_segment")

        logger_INC1A01.debug("Extracted Customer_Segment: %s", customer_segment)



        # Check if the filter condition is met: operator 'equal' and customer_segment is in rule_values
        if filter_04_details["operator"] == "equal" and customer_segment in filter_04_details["rule_values"]:
            logger_INC1A01.info("Customer segment filter triggered for Customer_Segment: %s", customer_segment)

            # Update incident details based on filter trigger
            incident_dict["Filtered_Reason"] = f"{filter_04_details['filter_rule']},customer_segment: {customer_segment}"

            logger_INC1A01.info("Incident rejected due to customer segment filter")

            logger_INC1A01.debug(incident_dict)
            return incident_dict

        else:
            logger_INC1A01.debug("Customer segment filter not triggered for Incident_Id: %s", incident_dict.get("Incident_Id"))
            return incident_dict

    except DataNotFoundError:
        logger_INC1A01.info("No customer_segment found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.exception("An error occurred while applying customer segment filter: %s", e)
        return incident_dict

# filter_15_details
def incident_filter_specific_customer_name(incident_dict):
    try:
        logger_INC1A01.debug("Starting specific customer name filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        filter_05_details = get_line_by_new_filter_id(15)
        logger_INC1A01.debug("Filter details: %s", filter_05_details)

        customer_name = incident_dict.get("Customer_Details", {}).get("Customer_Name")
        if not customer_name:
            raise DataNotFoundError("No Customer_Name")

        logger_INC1A01.debug("Extracted Customer_Name: %s", customer_name)



        match_found = any(
            re.search(rf'\b{re.escape(rule)}\b', customer_name, re.IGNORECASE)
            for rule in filter_05_details['rule_values']
        )

        if match_found:
            logger_INC1A01.info("Specific customer name filter triggered for Customer_Name: %s", customer_name)
            incident_dict["Filtered_Reason"] = f"{filter_05_details['filter_rule']},customer_name: {customer_name}"
            logger_INC1A01.info("Incident rejected due to specific customer name filter.")
            logger_INC1A01.debug("Updated incident: %s", incident_dict)
        else:
            logger_INC1A01.debug("No matching keyword found in Customer_Name for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        return incident_dict

    except DataNotFoundError:
        logger_INC1A01.info("No customer details found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.error("An error occurred while applying specific customer name filter: %s", e)
        return incident_dict


# filter_16_details
def incident_filter_specific_product_status(incident_dict):
    try:
        logger_INC1A01.debug("Starting special product status filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        # filter_06_details = {'new_filter_id': 16,'filter_rule': 'Specific Product Status','operator': 'equal','rule_values': ['SU'],'source_type': ['special', 'pilot suspend']}

        filter_06_details = get_line_by_new_filter_id(16)
        logger_INC1A01.debug("Filter details: %s", filter_06_details)

        # Extract product details
        product_list = incident_dict.get("Product_Details", [])
        if not product_list:
            raise DataNotFoundError("No Product_Details")

        logger_INC1A01.debug("Product_Details: %s", product_list)

        source_type = incident_dict.get("Source_Type")
        logger_INC1A01.debug("Source_Type: %s", source_type)

        for product in product_list:
            product_status = product.get("Product_Status")

            if product_status in filter_06_details["rule_values"]:
                if not source_type or source_type.lower() not in [stype.lower() for stype in filter_06_details["source_type"]]:
                    logger_INC1A01.info(f"Incident meets filter condition for Specific Product Status , product status: {product_status}, source_type: {source_type}",)

                    # Update incident
                    incident_dict["Filtered_Reason"] = f"{filter_06_details['filter_rule']}, product_status: {product_status}, source_type: {source_type}"


                    logger_INC1A01.debug("Incident rejected due to without specific source_type filter.")
                    return incident_dict
                else:
                    logger_INC1A01.debug("Product status matches but Source_Type is in skip list: %s", source_type)
                    return incident_dict

        logger_INC1A01.debug("No product matched the filter condition.")
        return incident_dict

    except DataNotFoundError:
        logger_INC1A01.info("No product details found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.exception("An unexpected error occurred: %s", e)
        return incident_dict




if __name__ == "__main__":
    initialize_hash_maps()

    print(40*"*")
    print(incident_filter_credit_class(incident_dict))

    print(40*"*")
    print(incident_filter_customer_type(incident_dict))

    print(40*"*")
    print(incident_filter_main_product_status(incident_dict))

    print(40*"*")
    print(incident_filter_customer_segment(incident_dict))

    print(40*"*")
    print(incident_filter_specific_customer_name(incident_dict))

    print(40 * "*")
    print(incident_filter_specific_product_status(incident_dict))



