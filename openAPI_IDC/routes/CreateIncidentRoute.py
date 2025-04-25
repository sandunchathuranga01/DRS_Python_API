"""
    Purpose:
    This module defines the FastAPI route for creating a new incident via the
    /Request_Incident_External_information endpoint.

    Description:
    It accepts an Incident model as input, calls the incident creation service,
    handles success and error cases (e.g., duplicate keys, validation issues),
    and returns appropriate HTTP responses along with logging duration and outcomes.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from datetime import datetime
from pymongo.errors import DuplicateKeyError
from openAPI_IDC.models.CreateIncidentModel import Incident
from openAPI_IDC.services.CreateIncidentService import create_incident, IncidentServiceResponse
from utils.customerExceptions.cust_exceptions import NotModifiedResponse
from utils.logger.loggers import get_logger
# endregion

# region Logger and Router Setup
logger_INC1A01 = get_logger('INC1A01')
router = APIRouter()
# endregion

# region Response Models
class IncidentResponse(BaseModel):
    Incident_Id: int
    message: str

class ErrorResponse(BaseModel):
    detail: str
# endregion

# region Endpoint
@router.post(
    "/Request_Incident_External_information",
    status_code=status.HTTP_201_CREATED,
    summary="Create a new incident",
    response_model=IncidentResponse,
    responses={
        201: {"description": "Incident created successfully", "model": IncidentResponse},
        409: {"description": "Duplicate Incident ID", "model": ErrorResponse},
        500: {"description": "Internal Server Error", "model": ErrorResponse},
    }
)
async def create_incident_endpoint(incident: Incident):
    """
    Endpoint to handle incoming incident creation requests.
    Calls the service function and returns appropriate HTTP responses.
    """

    logger_INC1A01.info(f"Received the incident data: /n {incident}")
    API_Start_time = datetime.now()

    # Call the service to create the incident
    result: IncidentServiceResponse = create_incident(incident)

    # Success case
    if result.success:
        incident_id = result.data
        logger_INC1A01.info(f"Incident created successfully (Incident_ID: {incident_id})")

        # Log processing time
        duration = (datetime.now() - API_Start_time).total_seconds()
        logger_INC1A01.info(f"Processing Duration: {duration:.6f} seconds")

        return {"Incident_Id": incident_id, "message": f"Incident {incident_id} created successfully"}

    # Duplicate incident ID
    if isinstance(result.error, DuplicateKeyError):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Incident with ID {incident.Incident_Id} was duplicated"
        )

    # Incident modification failed due to filter rules
    if isinstance(result.error, NotModifiedResponse):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"{result.error}"
        )

    # Any other unexpected error
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Unexpected error: {result.error}"
    )
# endregion
