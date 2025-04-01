from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ContactDetail(BaseModel):
    Contact_Type: str
    Contact: str
    Create_Dtm: datetime
    Create_By: str

class ProductDetail(BaseModel):
    Product_Label: str
    Customer_Ref: str
    Product_Seq: int
    Equipment_Ownership: Optional[str] = None
    Product_Id: str
    Product_Name: str
    Product_Status: str
    Effective_Dtm: datetime
    Service_Address: str
    Cat: str
    Db_Cpe_Status: Optional[str] = None
    Received_List_Cpe_Status: Optional[str] = None
    Service_Type: str
    Region: Optional[str] = None
    Province: Optional[str] = None

class CustomerDetail(BaseModel):
    Customer_Name: str
    Company_Name: Optional[str] = None
    Company_Registry_Number: Optional[str] = None
    Full_Address: str
    Zip_Code: str
    Customer_Type_Name: Optional[str] = None
    Nic: Optional[str] = None
    Customer_Type_Id: int
    Customer_Type: str

class AccountDetail(BaseModel):
    Account_Status: str
    Acc_Effective_Dtm: datetime
    Acc_Activate_Date: Optional[datetime] = None
    Credit_Class_Id: int
    Credit_Class_Name: str
    Billing_Centre: str
    Customer_Segment: str
    Mobile_Contact_Tel: Optional[str] = None
    Daytime_Contact_Tel: Optional[str] = None
    Email_Address: Optional[str] = None
    Last_Rated_Dtm: Optional[datetime] = None

class LastAction(BaseModel):
    Billed_Seq: Optional[int] = None
    Billed_Created: datetime
    Payment_Seq: Optional[int] = None
    Payment_Created: datetime
    Payment_Money: float
    Billed_Amount: float

class MarketingDetail(BaseModel):
    ACCOUNT_MANAGER: Optional[str] = None
    CONSUMER_MARKET: Optional[str] = None
    Informed_To: Optional[str] = None
    Informed_On: datetime

class Incident(BaseModel):
    Doc_Version: float = 1.0
    Incident_Id: int
    Account_Num: str
    Arrears: float
    arrears_band : Optional[str]
    Created_By: str
    Created_Dtm: datetime
    Incident_Status: Optional[str] = None
    Incident_Status_Dtm: Optional[datetime] = None
    Status_Description: Optional[str] = None
    File_Name_Dump: Optional[str] = None
    Batch_Id: Optional[str] = None
    Batch_Id_Tag_Dtm: Optional[datetime] = None
    External_Data_Update_On: Optional[datetime] = None
    Filtered_Reason: Optional[str] = None
    Export_On: Optional[datetime] = None
    File_Name_Rejected: Optional[str] = None
    Rejected_Reason: Optional[str] = None
    Incident_Forwarded_By: Optional[str] = None
    Incident_Forwarded_On: Optional[datetime] = None
    Contact_Details: List[ContactDetail]
    Product_Details: List[ProductDetail]
    Customer_Details: CustomerDetail
    Account_Details: AccountDetail
    Last_Actions: List[LastAction]
    Marketing_Details: List[MarketingDetail]
    Action: Optional[str] = None
    Validity_period: Optional[int] = None
    Remark: Optional[str] = None
    updatedAt: Optional[datetime] = None
    Rejected_By: Optional[str] = None
    Rejected_Dtm: Optional[datetime] = None
    Arrears_Band: Optional[str] = None
    Source_Type: Optional[str] = None
