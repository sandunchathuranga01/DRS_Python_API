import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from main import app

client = TestClient(app)

create_incident_service_PATH="openApi.routes.RequestIncidentExternalinformationRoute.create_incident_service"


@pytest.fixture
def sample_incident_data():
    return {
        "incident_id": "INC267",
        "account_no": "98J16OE2ZL",
        "Arrears": 0,
        "Created_By": "string",
        "Created_Dtm": "2025-02-02T14:14:27.429Z",
        "Incident_Status": "string",
        "Incident_Status_Dtm": "2025-02-02T14:14:27.429Z",
        "Status_Description": "string",
        "Contact_Details": [
            {
                "Contact_Type": "OFZESZDNCZUPLBSARDVIPIRTRJPROCPVHJSCHFSO",
                "Contact": 1000000000,
                "Create_Dtm": "2025-02-02T14:14:27.430Z",
                "Create_By": "string"
            }
        ],
        "Product_Details": [
            {
                "Product_Label": 0,
                "Customer_Ref": "string",
                "Product_Seq": "string",
                "Equipment_Ownership": "string",
                "Product_Id": 0,
                "Product_Name": "string",
                "Product_Status": "ACTIVE",
                "Effective_Dtm": "2025-02-02T14:14:27.430Z",
                "Service_Address": "string",
                "Cat": "string",
                "Db_Cpe_Status": "string",
                "Received_List_Cpe_Status": "string",
                "Service_Type": "string",
                "Region": "REGION1",
                "Province": "PROVINCE1"
            }
        ],
        "Customer_Details": {
            "Customer_Name": "string",
            "Company_Name": "string",
            "Company_Registry_Number": "string",
            "Full_Address": "string",
            "Zip_Code": 10000,
            "Customer_Type_Name": "string",
            "Nic": "360274405v",
            "Customer_Type_Id": 0
        },
        "Account_Details": {
            "Account_Status": "ACTIVE",
            "Acc_Effective_Dtm": "2025-02-02T14:14:27.430Z",
            "Acc_Activate_Date": "2025-02-02T14:14:27.430Z",
            "Credit_Class_Id": 0,
            "Credit_Class_Name": "string",
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


@patch(create_incident_service_PATH)
def test_create_incident_success(mock_create_incident_service, sample_incident_data):
    """Test successful creation of an incident with mock data."""

    # Ensure mock function is used instead of the real database call
    mock_create_incident_service.return_value = "mock_incident_id"

    # Call the API
    response = client.post("/Request_Incident_External_information", json=sample_incident_data)

    # Debugging output
    print("Response JSON:", response.json())

    # Validate response
    assert response.status_code == 200
    assert response.json() == {
        "message": "Incident created successfully",
        "incident_id": "mock_incident_id"
    }


@patch(create_incident_service_PATH)
def test_create_incident_failure(mock_create_incident_service, sample_incident_data):
    """Test failure scenario for incident creation."""

    # Simulate database failure
    mock_create_incident_service.side_effect = Exception("Database error")

    # Call the API
    response = client.post("/Request_Incident_External_information", json=sample_incident_data)

    # Debugging output
    print("Response JSON:", response.json())

    # Validate response
    assert response.status_code == 500
    assert "Error: Database error" in response.json()["detail"]
