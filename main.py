"""
    Purpose:
    This script initializes and runs the Incident Management API using FastAPI.

    Description:
    The main application file sets up the FastAPI server, configures global exception handlers,
    includes the incident creation routes, and defines a root route with a basic HTML welcome page.
    It also handles logging startup information and launches the server using uvicorn.

    Created Date: 2025-03-23
    Created By: Sandun Chathuranga(csandun@104@gmail.com)
    Last Modified Date: 2025-04-21
    Modified By: Sandun Chathuranga(csandun@104@gmail.com)
    Version: V1
"""

# region Imports
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import ValidationError
from openAPI_IDC.routes.CreateIncidentRoute import router as CreateIncidentRoute
import uvicorn
from utils.logger.loggers import get_logger
# endregion

# region Logger Initialization
logger_INC1A01 = get_logger('INC1A01')
# endregion

# region App Initialization
app = FastAPI()
# endregion

# region Exception Handlers
@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc):
    print(f"Validation error: {exc}")
    return JSONResponse(
        status_code=422,
        content={"detail": "Validation error", "errors": exc.errors()},
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    print(f"Internal server error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )
# endregion

# region Root Route
@app.get("/", response_class=HTMLResponse)
async def read_root():
    return """
    <html>
    <head>
        <title>Incident Management API</title>
    </head>
    <body style="font-family: Arial, sans-serif; text-align: center; margin-top: 50px;">
        <h1>Welcome to the Incident Management API</h1>
        <p>Use <b><a href="http://0.0.0.0:5000/docs" style="text-decoration: none; color: blue;">http://0.0.0.0:5000/docs</a></b> to explore the available endpoints.</p>
    </body>
    </html>
    """
# endregion

# region Route Inclusions
app.include_router(CreateIncidentRoute)
# endregion

# region Main Function
def main():
    logger_INC1A01.info("Starting Incident Management API")
    #uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)

if __name__ == "__main__":
    main()
# endregion
