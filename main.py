from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import ValidationError

from openAPI_IDC.routes.CreateIncidentRoute import router as CreateIncidentRoute
import uvicorn
from utils.logger.loggers import get_logger

# Get the logger
logger = get_logger("System_logger")


# Initialize FastAPI application
app = FastAPI()

# Exception Handlers
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

# Root route with a simple design
@app.get("/", response_class=HTMLResponse)
async def read_root():
    return """
    <html>
    <head>
        <title>Incident Management API</title>
    </head>
    <body style="font-family: Arial, sans-serif; text-align: center; margin-top: 50px;">
        <h1>Welcome to the Incident Management API</h1>
        <p>Use <b><a href="http://127.0.0.1:8000/docs" style="text-decoration: none; color: blue;">http://127.0.0.1:8000/docs</a></b> to explore the available endpoints.</p>
    </body>
    </html>
    """

# Include routes
app.include_router(CreateIncidentRoute)

def main():
    logger.info("Starting Incident Management API")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

if __name__ == "__main__":
    main()


