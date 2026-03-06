# Import FastAPI components
from fastapi import FastAPI
from pydantic import BaseModel
import sys

# Insert project path for correct relative imports
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

# Import core Agent classes
from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter

# Initialize the FastAPI App instance
app = FastAPI()

print("Initializing NO-LLM Agent Orchestrator...")
# Fast initialization of Orchestrator without loading LLM (Local Rule-based engine only)
# This is much faster and uses fewer resources if AI suggestions are not required
orchestrator = AgentOrchestrator(use_llm=False, use_explain=False)
# Formatter to convert model output to dictionary form
formatter = OutputFormatter()
print("Agent Ready!")

# Define the expected JSON payload schema for the POST endpoint
class QueryRequest(BaseModel):
    query: str

# Define the analysis endpoint
@app.post("/analyze")
def analyze(req: QueryRequest):
    # Optimize the passed query using just rule-based strategies
    result = orchestrator.optimize(req.query)
    # Convert result to JSON-serializable dictionary
    output = formatter.format_as_json(result)
    return output

# Run standard ASGI Uvicorn server if script executed directly
if __name__ == "__main__":
    import uvicorn
    # Notice this server binds to port 8010 rather than 8009
    uvicorn.run(app, host="0.0.0.0", port=8010)
