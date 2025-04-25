"""
    Purpose:
    Applies Level 2 filtering on incident data based on the arrears amount.

    Description:
    Uses fixed business rules to determine the appropriate `Incident_Status` and
    `Status_Description` based on the `Arrears` value provided in the incident_dict.
    This is considered the fallback logic when F1 filters (Level 1) do not reject the incident.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from datetime import datetime
from utils.logger.loggers import get_logger
# endregion

# region Logger
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region Select_Criteria Filter Function
def Select_Criteria_for_incident_dict(incident_dict):
    """
    Applies Level 2 filtering to the incident based on 'Arrears' value.

    Logic:
        - If arrears > 5000         → Status = 'Open No agent'
        - If 1000 < arrears <= 5000 → Status = 'Direct LOD'
        - If arrears <= 1000        → Status = 'Open CPE Collect'
        - Else                      → Status = 'Unknown'

    Sets:
        - 'Incident_Status'
        - 'Status_Description'
        - 'Incident_Status_Dtm'

    Returns:
        dict: Updated incident_dict with filtering results.
    """
    try:
        # Get the arrears value from the incident
        arrears = incident_dict.get("Arrears")

        # Apply filtering based on arrears amount
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

        # Add the current datetime to the incident
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()

        # Log the updated incident dictionary
        logger_INC1A01.debug(incident_dict)
        return incident_dict

    except Exception as e:
        # Log error and set incident status as 'Error'
        logger_INC1A01.error(e)
        incident_dict["Incident_Status"] = "Error"
        incident_dict["Incident_Status_Dtm"] = datetime.now().isoformat()
        incident_dict["Status_Description"] = str(e)
        return incident_dict
# endregion
