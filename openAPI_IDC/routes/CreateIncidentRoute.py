from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from datetime import datetime
from pymongo.errors import DuplicateKeyError

from openAPI_IDC.models.CreateIncidentModel import Incident
from openAPI_IDC.services.CreateIncidentService import create_incident
from utils.logger.loggers import get_logger

# Get the logger
logger_INC1A01 = get_logger('INC1A01')

router = APIRouter()

# Pydantic Models for Responses
class IncidentResponse(BaseModel):
    Incident_Id: int
    message: str

class ErrorResponse(BaseModel):
    detail: str


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
    try:
        logger_INC1A01.info("Starting the incident creation process.")
        API_Start_time = datetime.now()

        incident_id = create_incident(incident)

        if incident_id == -1:
            raise Exception("Unknown error occurred while creating the incident.")
        elif incident_id == -2:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Incident with ID {incident.Incident_Id} already exists"
            )
        elif incident_id == -3:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail= f"Incident with ID {incident.Incident_Id} was not filtered"
            )

        logger_INC1A01.info(f"Incident created successfully (id: {incident_id})")
        API_End_time = datetime.now()
        processing_time = (API_End_time - API_Start_time).total_seconds()
        logger_INC1A01.info(f"Processing Duration: {processing_time:.6f} seconds")

        return {"Incident_Id": incident_id, "message": "Incident created successfully"}

    except HTTPException as http_err:
        raise http_err  # Re-raise HTTP errors (such as 409 Conflict) properly

    except Exception as e:
        logger_INC1A01.error(f"Error creating incident: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create incident",
        )
