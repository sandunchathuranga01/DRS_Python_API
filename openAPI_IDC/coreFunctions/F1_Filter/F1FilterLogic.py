"""
    Purpose:
    Defines F1 filter logic functions to apply filtering rules on incoming incidents.

    Description:
    Each filter function (Filter 11 to 16) checks specific fields like credit class, customer type,
    product status, etc., and updates the incident dictionary if it matches the configured filter rule.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
import re
from datetime import datetime
from utils.customerExceptions.cust_exceptions import DataNotFoundError
from utils.logger.loggers import get_logger
# endregion

# region Logger
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Filter 11: Credit Class
def incident_filter_credit_class(incident_dict,filter_details):
    """
    Applies filter based on Credit_Class_Id from Account_Details.
    Checks if the value matches predefined rule_values.
    """
    filter_data= {}

    try:
        logger_INC1A01.debug("Starting incident filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        # Fetch filter configuration details using new_filter_id 11
        for details in filter_details.values():
            if details.get('new_filter_id') == 11:
                filter_data = details
                break

        logger_INC1A01.debug("Filter details: %s", filter_data)

        # Extract credit class from incident
        credit_class_id = incident_dict.get("Account_Details", {}).get("Credit_Class_Id")
        if not credit_class_id:
            raise DataNotFoundError("No credit class id")

        # Apply filter rule
        if filter_data["operator"] == "equal" and credit_class_id in filter_data["rule_values"]:
            # Add reason for filtering
            incident_dict["Filtered_Reason"] = f"Incident meets filter condition for {filter_data['filter_rule']},Credit_Class_Id: {credit_class_id}"
            logger_INC1A01.info("Incident meets filter condition for Credit_Class_Id: %s", credit_class_id)
            return incident_dict
        else:
            return incident_dict

    except DataNotFoundError:
        logger_INC1A01.debug("No credit class id found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        # Handle unexpected errors
        logger_INC1A01.exception("An unexpected error occurred: %s", e)
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict

    finally:
        # Clean up or log that function execution is complete
        filter_data.clear()
# endregion

# region Filter 12: Customer Type
def incident_filter_customer_type(incident_dict,filter_details):
    """
    Applies filter based on Customer_Type_Name.
    """
    filter_data= {}

    try:
        logger_INC1A01.debug("Starting customer type filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        # Fetch filter configuration details using new_filter_id 12
        for details in filter_details.values():
            if details.get('new_filter_id') == 12:
                filter_data = details
                break

        logger_INC1A01.debug("Filter details: %s", filter_data)

        # Extract customer type from incident
        customer_type_name = incident_dict.get("Customer_Details", {}).get("Customer_Type_Name")
        if not customer_type_name:
            raise DataNotFoundError("No customer type name")

        # Apply filter rule
        if filter_data["operator"] == "equal" and customer_type_name in filter_data["rule_values"]:
            incident_dict["Filtered_Reason"] = f"{filter_data['filter_rule']},customer_type_name: {customer_type_name}"
            logger_INC1A01.info("Incident rejected due to customer type filter")
            return incident_dict
        else:
            return incident_dict

    except DataNotFoundError:
        logger_INC1A01.debug("No customer type name found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.exception("An error occurred while applying customer type filter: %s", e)
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict

    finally:
        filter_data.clear()
# endregion

# region Filter 13: Main Product Status
def incident_filter_main_product_status(incident_dict,filter_details):
    """
    Applies filter if any Product_Status in Product_Details matches rule_values.
    """
    filter_data= {}

    try:
        logger_INC1A01.debug("Starting main product status filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))
        # Fetch filter configuration details using new_filter_id 13
        for details in filter_details.values():
            if details.get('new_filter_id') == 13:
                filter_data = details
                break

        logger_INC1A01.debug("Filter details: %s", filter_data)

        # Extract product details list
        product_details = incident_dict.get("Product_Details", [])
        if not product_details:
            raise DataNotFoundError("No product details")

        # Check if any product status matches rule values
        match_found = any(
            product.get("Product_Status") in filter_data["rule_values"]
            for product in product_details
        )

        if filter_data["operator"] == "equal" and match_found:
            matching_statuses = [p["Product_Status"] for p in product_details if p.get("Product_Status") in filter_data["rule_values"]]
            incident_dict["Filtered_Reason"] = f"{filter_data['filter_rule']}, matched_statuses: {matching_statuses}"
            logger_INC1A01.info("Incident rejected due to main product status filter.")
            return incident_dict
        else:
            return incident_dict

    except DataNotFoundError:
        logger_INC1A01.info("No product details found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.error("Unexpected error occurred: %s", e)
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict

    finally:
        # Clean up or log that function execution is complete
        filter_data.clear()
# endregion

# region Filter 14: Customer Segment
def incident_filter_customer_segment(incident_dict,filter_details):
    """
    Applies filter based on Customer_Segment from Account_Details.
    """
    filter_data= {}

    try:
        logger_INC1A01.debug("Starting customer segment filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        # Fetch filter configuration details using new_filter_id 14
        for details in filter_details.values():
            if details.get('new_filter_id') == 14:
                filter_data = details
                break

        logger_INC1A01.debug("Filter details: %s", filter_data)

        # Extract customer segment
        customer_segment = incident_dict.get("Account_Details", {}).get("Customer_Segment")
        if not customer_segment:
            raise DataNotFoundError("No customer_segment")

        # Apply filter
        if filter_data["operator"] == "equal" and customer_segment in filter_data["rule_values"]:
            incident_dict["Filtered_Reason"] = f"{filter_data['filter_rule']},customer_segment: {customer_segment}"
            logger_INC1A01.info("Incident rejected due to customer segment filter")
            return incident_dict
        else:
            return incident_dict

    except DataNotFoundError:
        logger_INC1A01.info("No customer_segment found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.exception("An error occurred while applying customer segment filter: %s", e)
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict

    finally:
        # Clean up or log that function execution is complete
        filter_data.clear()
# endregion

# region Filter 15: Specific Customer Name
def incident_filter_specific_customer_name(incident_dict,filter_details):
    """
    Applies regex-based filter to check if Customer_Name contains any configured keywords.
    """
    filter_data= {}

    try:
        logger_INC1A01.debug("Starting specific customer name filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))
        # Fetch filter configuration details using new_filter_id 15
        for details in filter_details.values():
            if details.get('new_filter_id') == 15:
                filter_data = details
                break

        logger_INC1A01.debug("Filter details: %s", filter_data)

        # Extract customer name
        customer_name = incident_dict.get("Customer_Details", {}).get("Customer_Name")
        if not customer_name:
            raise DataNotFoundError("No Customer_Name")

        # Perform regex search for keywords
        match_found = any(
            re.search(rf'\b{re.escape(rule)}\b', customer_name, re.IGNORECASE)
            for rule in filter_data['rule_values']
        )

        if match_found:
            incident_dict["Filtered_Reason"] = f"{filter_data['filter_rule']},customer_name: {customer_name}"
            logger_INC1A01.info("Incident rejected due to specific customer name filter.")
        return incident_dict

    except DataNotFoundError:
        logger_INC1A01.info("No customer details found for Incident_Id: %s", incident_dict.get("Incident_Id"))
        return incident_dict

    except Exception as e:
        logger_INC1A01.error("An error occurred while applying specific customer name filter: %s", e)
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict

    finally:
        # Clean up or log that function execution is complete
        filter_data.clear()
# endregion

# region Filter 16: Specific Product Status with Source Type Check
def incident_filter_specific_product_status(incident_dict,filter_details):
    """
    Applies filter to Product_Details based on Product_Status and optional Source_Type validation.
    """
    filter_data= {}

    try:
        logger_INC1A01.debug("Starting special product status filter for Incident_Id: %s", incident_dict.get("Incident_Id", "Unknown"))

        # Fetch filter configuration details using new_filter_id 16
        for details in filter_details.values():
            if details.get('new_filter_id') == 16:
                filter_data = details
                break

        logger_INC1A01.debug("Filter details: %s", filter_data)

        # Extract product and source type details
        product_list = incident_dict.get("Product_Details", [])
        if not product_list:
            raise DataNotFoundError("No Product_Details")

        source_type = incident_dict.get("Source_Type")

        for product in product_list:
            product_status = product.get("Product_Status")

            if product_status in filter_data["rule_values"]:
                # Reject if source_type is not in allowed list
                if not source_type or source_type.lower() not in [stype.lower() for stype in filter_data["source_type"]]:
                    incident_dict["Filtered_Reason"] = f"{filter_data['filter_rule']}, product_status: {product_status}, source_type: {source_type}"
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
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict

    finally:
        # Clean up or log that function execution is complete
        filter_data.clear()
# endregion
