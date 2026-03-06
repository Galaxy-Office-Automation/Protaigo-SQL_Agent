# Import standard library modules used for system operations and logging
import sys
import logging
# Import FastAPI and web server components
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import warnings

# Ignore any warnings that might clutter the logs/output
warnings.filterwarnings("ignore")

# Configure the logging format to include timestamp, severity level, and the log message
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# Create a logger specifically for the API
logger = logging.getLogger("sql_agent_api")

# Add the project root directory to the python path so module imports work correctly
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

import os
# Hardcode OpenAI API Key for demonstration purposes (unsafe for production!)
os.environ["OPENAI_API_KEY"] = "sk-proj-xyz"

# Attempt to import the main components from our agent modules
try:
    from agent.orchestrator import AgentOrchestrator
    from output.formatter import OutputFormatter
except ImportError as e:
    # Log an error if the modules cannot be found
    logger.error(f"Import error: {e}")

# Initialize the FastAPI application with a title and version
app = FastAPI(title="SQL Optimizer Agent API", version="1.0.0")

# Global variables to hold the orchestrator and formatter instances
_orchestrator = None
_formatter = None

# This event runs once when the application starts
@app.on_event("startup")
async def startup_event():
    global _orchestrator, _formatter
    logger.info("Initializing AgentOrchestrator...")
    try:
        # Instantiate the orchestrator with LLM enabled but disable slow EXPLAIN
        _orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
        # Instantiate the output formatter to structure output responses
        _formatter = OutputFormatter()
        logger.info("AgentOrchestrator initialized!")
    except Exception as e:
        # Log failure to start the orchestrator
        logger.error(f"Failed to initialize AgentOrchestrator: {e}")

# This event runs when the application shuts down
@app.on_event("shutdown")
async def shutdown_event():
    global _orchestrator
    # Close any database connections or resources held by the orchestrator
    if _orchestrator:
        _orchestrator.close()

# Pydantic model for validating incoming JSON requests containing the SQL query
class QueryRequest(BaseModel):
    query: str

# Health check endpoint to ensure the API is running and ready
@app.get("/health")
async def health_check():
    # If the orchestrator failed to load, return 503 Service Unavailable
    if _orchestrator is None:
        raise HTTPException(status_code=503, detail="AgentOrchestrator not initialized")
    return {"status": "ok", "message": "SQL Agent API is healthy."}

# The main optimization API endpoint for analyzing queries
@app.post("/analyze")
async def analyze_query(request: QueryRequest):
    # Check if the requested query string is empty
    if not request.query or not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    # Check if the orchestrator is ready to process queries
    if _orchestrator is None:
        raise HTTPException(status_code=503, detail="Agent not loaded")
    
    try:
        # Pass the query to the orchestrator to get the optimization suggestion
        result = _orchestrator.optimize(request.query)
        # Format the result back as a JSON object
        return _formatter.format_as_json(result)
    except Exception as e:
        # If any internal error happens, log it and return a 500 error
        logger.error(f"Error analyzing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Running the app via uvicorn if the script is executed directly
if __name__ == "__main__":
    uvicorn.run("api_server:app", host="0.0.0.0", port=5051, log_level="info")
