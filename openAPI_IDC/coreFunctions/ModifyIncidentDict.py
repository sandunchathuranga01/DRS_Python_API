"""
    Purpose:
    Handles the full pipeline of F1 filtering and data enrichment for incoming incidents.

    Description:
    - The `do_f1_filter_for_incident_dict` function applies Level 1 and Level 2 filters.
    - The `get_modified_incident_dict` function:
        • Checks for existing open accounts
        • Links related accounts
        • Assigns arrears band
        • Applies Level 1 and Level 2 filtering
    - This module ensures all rules and configurations are dynamically loaded and applied.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from datetime import datetime

from openAPI_IDC.coreFunctions.ConfigManager import initialize_hash_maps
from openAPI_IDC.coreFunctions.DatabaseOparations.CheckAccount import has_open_case_for_account, link_accounts_from_open_accounts
from openAPI_IDC.coreFunctions.F1_Filter.F1Filter import do_f1_filter
from openAPI_IDC.coreFunctions.AssignArrearsBand import assign_arrears_band
from openAPI_IDC.coreFunctions.F1_Filter.SelectCriteria import Select_Criteria_for_incident_dict
from openAPI_IDC.coreFunctions.DatabaseOparations.GetF1FilterDetails import get_new_filter_id_list_from_active_filters, get_active_filters
from openAPI_IDC.coreFunctions.F1_Filter.example_incident_dict import incident_dict
from utils.customerExceptions.cust_exceptions import DataNotFoundError, NotModifiedResponse, AccountNumberAlreadyExists
from utils.logger.loggers import get_logger
# endregion

# region Logger
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region F1 Filtering Orchestrator
def do_f1_filter_for_incident_dict(incident_dict):
    """
    Applies Level 1 and Level 2 filters on the incident_dict based on filter configuration.

    If a Level 1 filter is triggered, the incident is marked as 'reject pending'.
    If no Level 1 filters match, Level 2 filter is applied to determine final status.

    Args:
        incident_dict (dict): The incident to process.

    Returns:
        dict: The updated incident_dict with filtering status and reason.
    """
    try:
        # Retrieve all active filter details
        filter_details = get_active_filters()

        # Retrieve all active filter IDs
        filter_ids = get_new_filter_id_list_from_active_filters()

        if not filter_ids:
            raise DataNotFoundError("Missing Filter IDs")

        # Apply each Level 1 filter
        for filter_id in filter_ids:
            logger_INC1A01.debug(f"Running filter ID: {filter_id}")
            updated_incident = do_f1_filter(filter_id, incident_dict, filter_details)

            # If an error occurred in filtering, stop
            if updated_incident.get("Incident_Status") == "Error":
                logger_INC1A01.error(f"Error encountered at filter ID {filter_id}. Stopping further filtering.")
                return updated_incident

            # If a filter matched, reject the incident
            if updated_incident.get("Filtered_Reason"):
                updated_incident["Incident_Status"] = "reject pending"
                updated_incident["Incident_Status_Dtm"] = datetime.now().isoformat()
                logger_INC1A01.info(
                    f"Incident rejected at filter_id {filter_id} with reason: {updated_incident['Filtered_Reason']}")
                return updated_incident

        return incident_dict

    except DataNotFoundError:
        logger_INC1A01.exception("Missing Filter IDs")
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = "Missing Filter IDs"
        return incident_dict

    except Exception as e:
        logger_INC1A01.exception(f"Unexpected error during F1 filtering: {e}")
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = str(e)
        return incident_dict
# endregion

# region Incident Modifier
def get_modified_incident_dict(incident_dict):
    """
    Modifies an incident dictionary by:
    - Checking if an open case already exists for the account
    - Linking related account numbers
    - Assigning an arrears band
    - Applying F1 filters (level 1 and 2)

    Args:
        incident_dict (dict): The original incident to be enriched and processed.

    Returns:
        dict: Updated incident_dict with status, description, and filter outcome.
    """
    try:
        # Check if account already exists as open cases
        if has_open_case_for_account(incident_dict) is True:
            raise AccountNumberAlreadyExists(f"{incident_dict.get('account_number')} already exists in open cases")

        # Add linked accounts with respect to customer_ref
        incident_dict_with_link_accounts = link_accounts_from_open_accounts(incident_dict)

        # Stop if an error occurred during arrears assignment
        if incident_dict_with_link_accounts.get("Incident_Status") == "Error":
            raise NotModifiedResponse(f"Error Occurred during the link accounts {incident_dict_with_link_accounts}")

        # Assign arrears band using configured bands
        incident_dict_with_arrears_band = assign_arrears_band(incident_dict_with_link_accounts)

        # Stop if an error occurred during arrears assignment
        if incident_dict_with_arrears_band.get("Incident_Status") == "Error":
            raise NotModifiedResponse(f"Arrears band not modified: {incident_dict_with_arrears_band}")

        # Apply filtering rules
        F1_Filter_Results = do_f1_filter_for_incident_dict(incident_dict_with_arrears_band)
        logger_INC1A01.debug(f"F1 result: {F1_Filter_Results}")

        if F1_Filter_Results.get("Incident_Status") == "Error":
            raise NotModifiedResponse(f"Arrears band not modified: {F1_Filter_Results}")

        # If already filtered before, don't apply Select_Criteria
        if F1_Filter_Results.get("Filtered_Reason"):
            logger_INC1A01.info("Incident already filtered by level 1 filter. Skipping level 2 filtering.")
            return F1_Filter_Results

        # If no F1 filter triggered, apply Level Select_Criteria
        logger_INC1A01.info("No filter triggered at F1 filter. Running Select_Criteria...")
        Select_Criteria_result = Select_Criteria_for_incident_dict(incident_dict)

        if Select_Criteria_result.get("Incident_Status") == "Error":
            raise NotModifiedResponse(f"Arrears band not modified: {Select_Criteria_result}")

        logger_INC1A01.info(f"Select criteria result{Select_Criteria_result}")
        return Select_Criteria_result


    except AccountNumberAlreadyExists as account_number_already_exists:
        logger_INC1A01.info(str(account_number_already_exists))
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = account_number_already_exists
        return incident_dict

    except NotModifiedResponse as e:
        logger_INC1A01.error(f"No valid data found. Reason: {e}")
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict

    except Exception as e:
        logger_INC1A01.error(f"Unexpected error: {e}")
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = e
        return incident_dict
# endregion


if __name__ == '__main__':
    initialize_hash_maps()
    print(get_modified_incident_dict(incident_dict))
