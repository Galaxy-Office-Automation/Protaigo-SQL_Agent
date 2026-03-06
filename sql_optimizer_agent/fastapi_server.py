# Import FastAPI application class and request object
from fastapi import FastAPI, Request
# Import BaseModel from pydantic to define data schemas
from pydantic import BaseModel
import sys
import json

# Add the project root directory to sys.path to resolve module imports
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

# Import core application logic
from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter

# Initialize the FastAPI App instance
app = FastAPI()

print("Initializing Agent Orchestrator... (this takes ~16s but only happens ONCE)")
# Initialize the Orchestrator ONCE during global startup with LLM enabled
orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
# Initialize the Output formatter for JSON responses
formatter = OutputFormatter()
print("Agent Ready!")

# Define the expected JSON payload schema for the API
class QueryRequest(BaseModel):
    query: str

# Define the /analyze endpoint using POST
@app.post("/analyze")
def analyze(req: QueryRequest):
    try:
        # Running as `def` instead of `async def` correctly offloads blocking calls to a threadpool in FastAPI!
        # This prevents a slow LLM operation from blocking other requests.
        result = orchestrator.optimize(req.query)
        # Format the processed orchestration output into a JSON dictionary
        output = formatter.format_as_json(result)
        return output
    except Exception as e:
        # Return any exception cleanly as json to the caller
        return {"error": str(e)}

# Entry point to run the server via Uvicorn
if __name__ == "__main__":
    import uvicorn
    # Listens on all interfaces at port 8009
    uvicorn.run(app, host="0.0.0.0", port=8009)
