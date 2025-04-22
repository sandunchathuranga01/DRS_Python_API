import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from main import app  # Import FastAPI app

client = TestClient(app)

update_incident_service_PATH ="openApi.routes.RequestIncidentExternalinformationRoute.update_incident_service"

@pytest.fixture
def sample_incident_data():
    return {
        "incident_id": "INC267",
        "Incident_Status": "Resolved",
        "Status_Description": "Issue has been fixed",
        "Customer_Details": {
            "Customer_Name": "John Doe",
            "Company_Name": "Tech Corp",
            "Company_Registry_Number": "123456",
            "Full_Address": "123 Main St",
            "Zip_Code": 10000,
            "Customer_Type_Name": "Corporate",
            "Nic": "360274405v",
            "Customer_Type_Id": 1
        },
        "Account_Details": {
            "Account_Status": "ACTIVE",
            "Acc_Effective_Dtm": "2025-02-02T14:14:27.430Z",
            "Acc_Activate_Date": "2025-02-02T14:14:27.430Z",
            "Credit_Class_Id": 0,
            "Credit_Class_Name": "Standard",
            "Billing_Centre": "CENTER1",
            "Customer_Segment": 0,
            "Mobile_Contact_Tel": "string",
            "Daytime_Contact_Tel": "string",
            "Email_Address": "user@example.com",
            "Last_Rated_Dtm": "2025-02-02T14:14:27.430Z"
        },
        "Last_Actions": {
            "Billed_Seq": "string",
            "Billed_Created": "2025-02-02T14:14:27.430Z",
            "Payment_Seq": "string",
            "Payment_Created": "2025-02-02T14:14:27.430Z"
        },
        "Marketing_Details": {
            "ACCOUNT_MANAGER": "string",
            "CONSUMER_MARKET": "string",
            "Informed_To": "string",
            "Informed_On": "2025-02-02T14:14:27.430Z"
        }
    }

@patch(update_incident_service_PATH)
def test_update_incident_success(mock_update_incident_service, sample_incident_data):
    """Test successful update of an incident with mock data."""

    # Ensure mock function is used instead of the real database call
    mock_update_incident_service.return_value = ["Incident_Status", "Status_Description"]

    # Call the API
    response = client.patch("/Request_Incident_External_information", json=sample_incident_data)

    # Debugging output
    print("Response JSON:", response.json())

    # Validate response
    assert response.status_code == 200
    assert response.json() == {
        "message": "Incident updated successfully",
        "updated_fields": ["Incident_Status", "Status_Description"]
    }


@patch(update_incident_service_PATH)
def test_update_incident_missing_id(mock_update_incident_service):
    """Test failure scenario when `incident_id` is missing."""

    invalid_data = {
        "Incident_Status": "Resolved",
        "Status_Description": "Issue has been fixed"
    }

    # Call the API
    response = client.patch("/Request_Incident_External_information", json=invalid_data)

    # Debugging output
    print("Response JSON:", response.json())

    # Validate response
    assert response.status_code == 400
    assert response.json() == {"detail": "incident_id is required in the request body"}


@patch(update_incident_service_PATH)
def test_update_incident_not_found(mock_update_incident_service, sample_incident_data):
    """Test failure scenario when `incident_id` does not exist."""

    # Simulate database returning an error
    mock_update_incident_service.side_effect = Exception("Incident ID not found")

    # Call the API
    response = client.patch("/Request_Incident_External_information", json=sample_incident_data)

    # Debugging output
    print("Response JSON:", response.json())

    # Validate response
    assert response.status_code == 500
    assert response.json() == {"detail": "Error: Incident ID not found"}


@patch(update_incident_service_PATH)
def test_update_incident_no_fields_provided(mock_update_incident_service):
    """Test failure scenario when no fields are provided for update."""

    only_id_data = {"incident_id": "INC267"}

    # Simulate the update service raising an error
    mock_update_incident_service.side_effect = Exception("No valid fields provided for update")

    # Call the API
    response = client.patch("/Request_Incident_External_information", json=only_id_data)

    # Debugging output
    print("Response JSON:", response.json())

    # Validate response
    assert response.status_code == 500
    assert response.json() == {"detail": "Error: No valid fields provided for update"}
