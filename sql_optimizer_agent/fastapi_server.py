
from fastapi import FastAPI, Request
from pydantic import BaseModel
import sys
import json
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter

app = FastAPI()

print("Initializing Agent Orchestrator... (this takes ~16s but only happens ONCE)")
orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
formatter = OutputFormatter()
print("Agent Ready!")

class QueryRequest(BaseModel):
    query: str

@app.post("/analyze")
def analyze(req: QueryRequest):
    try:
        # Running as `def` instead of `async def` correctly offloads blocking calls to a threadpool in FastAPI!
        result = orchestrator.optimize(req.query)
        output = formatter.format_as_json(result)
        return output
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5051)
