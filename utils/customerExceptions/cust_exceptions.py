class DatabaseConnectionError(Exception):
    """Raised when the connection to the databaseOperations fails."""
    def __init__(self, message="Database connection failed"):
        super().__init__(message)

class DataNotFoundError(Exception):
    """Raised when the not found request data"""
    def __init__(self, message="Data not found"):
        super().__init__(message)

class DataFetchError(Exception):
    """Raised when an error occurs while fetching the data."""
    def __init__(self, message="An error occurred while fetching the Data"):
        super().__init__(message)

class DataInsertError(Exception):
    """Raised when an error occurs while retrieving data."""
    def __init__(self, message="An error occurred while retrieving data"):
        super().__init__(message)


class NoValidDataError(Exception):
    """Raised when no valid data is available for insertion."""
    def __init__(self, message="No valid data to insert"):
        super().__init__(message)

class ProcessingError(Exception):
    """Raised when an error occurs while processing an individual task."""
    def __init__(self, message="Error processing task"):
        full_message = f"{message}"
        super().__init__(full_message)

class TaskIdNotFoundError(Exception):
    def __init__(self, message="task id Not found"):
        full_message = f"{message}"
        super().__init__(full_message)

class CaseIdNotFoundError(Exception):
    def __init__(self, message="case id Not found"):
        full_message = f"{message}"
        super().__init__(full_message)

class FileMissingError(Exception):
    def __init__(self, message="case id Not found"):
        full_message = f"{message}"
        super().__init__(full_message)

class DocumentUpdateError(Exception):
    def __init__(self, message="Document update failed"):
        full_message = f"{message}"
        super().__init__(full_message)


class NotModifiedResponse(Exception):
    def __init__(self, message="Not Modified Response"):
        full_message = f"{message}"
        super().__init__(full_message)



