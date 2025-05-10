from pydantic import BaseModel
from typing import List, Optional,Any
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

class AccountCrossDetails(BaseModel):
    Incident_Id: Optional[int] = None
    Case_Id: Optional[int] = None
    Account_Num: str
    Account_Status: Optional[str] = None
    Outstanding_Balance:float

class LastAction(BaseModel):
    Billed_Seq: Optional[int] = None
    Billed_Created: datetime
    Payment_Seq: Optional[int] = None
    Payment_Created: datetime
    Payment_Money: float
    Billed_Amount: float

class MarketingDetail(BaseModel):
    Account_Manager: Optional[str] = None
    Consumer_Market: Optional[str] = None
    Informed_To: Optional[str] = None
    Informed_On: Optional[datetime] = None

class Incident(BaseModel):
    Doc_Version: float = 1.0
    Incident_Id: Optional[int] = None
    Account_Num: str
    Customer_Ref : str
    Arrears: float
    Arrears_Band : Optional[str]
    Bss_Arrears_Amount : float
    Last_Bss_Reading_Date : datetime
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
    Account_Cross_Details: Optional[List[AccountCrossDetails]] = None
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
    Source_Type: Optional[str] = None
    Ref_Data_Temp_Permanent: Optional[List[Any]] = None,
    Case_Status: Optional[List[Any]] = None,
    Approvals: Optional[List[Any]] = None,
    DRC: Optional[List[Any]] = None,
    RO: Optional[List[Any]] = None,
    RO_Requests: Optional[List[Any]] = None,
    RO_Negotiation: Optional[List[Any]] = None,
    RO_Customer_Details_Edit: Optional[List[Any]] = None,
    RO_CPE_Collect: Optional[List[Any]] = None,
    Mediation_Board: Optional[List[Any]] = None,
    Settlement: Optional[List[Any]] = None,
    Money_Transactions: Optional[List[Any]] = None,
    Commission_Bill_Payment: Optional[List[Any]] = None,
    Bonus: Optional[List[Any]] = None,
    FTL_LOD: Optional[List[Any]] = None,
    Litigation: Optional[List[Any]] = None,
    LOD_Final_Reminder: Optional[List[Any]] = None,
    Dispute: Optional[List[Any]] = None,
    Abnormal_Stop: Optional[List[Any]] = None
