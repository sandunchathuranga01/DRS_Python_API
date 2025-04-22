"""
    Purpose:
    This module dispatches the incoming incident to the appropriate F1 filter function
    based on the `new_filter_id`.

    Description:
    - Uses Python match-case to determine which filtering rule to apply.
    - Routes incident data to individual filter functions defined in `F1FilterLogic`.
    - Logs when no matching filter rule is found.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from openAPI_IDC.coreFunctions.F1_Filter.F1FilterLogic import (
    incident_filter_customer_type,
    incident_filter_credit_class,
    incident_filter_main_product_status,
    incident_filter_customer_segment,
    incident_filter_specific_customer_name,
    incident_filter_specific_product_status
)
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Filter Dispatcher Function
def do_f1_filter(value, incident_dict, filter_details):
    """
    Dispatches the incident_dict to the appropriate F1 filter function
    based on the provided filter value (new_filter_id).

    Args:
        value (int): The filter ID used to select the filter rule.
        incident_dict (dict): The incident data to be evaluated.
        filter_details (dict): Dictionary containing all filter configurations.

    Returns:
        dict: The updated incident_dict after applying the relevant filter.
    """
    match value:
        case 11:
            # Apply Credit Class filter
            return incident_filter_credit_class(incident_dict, filter_details)
        case 12:
            # Apply Customer Type filter
            return incident_filter_customer_type(incident_dict, filter_details)
        case 13:
            # Apply Main Product Status filter
            return incident_filter_main_product_status(incident_dict, filter_details)
        case 14:
            # Apply Customer Segment filter
            return incident_filter_customer_segment(incident_dict, filter_details)
        case 15:
            # Apply Specific Customer Name filter
            return incident_filter_specific_customer_name(incident_dict, filter_details)
        case 16:
            # Apply Specific Product Status with source type check
            return incident_filter_specific_product_status(incident_dict, filter_details)
        case _:
            # If no valid filter is found, return as-is with info log
            logger_INC1A01.error(f"No filter found for value: {value}")
            return incident_dict
# endregion
