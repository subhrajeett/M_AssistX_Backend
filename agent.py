"""
Manufacturing Downtime AI Agent — Backend
==========================================
Stack : FastAPI  +  Strands Agents  +  AWS Bedrock  +  pyodbc (MSSQL)

Tables (your actual schema):
  ProductionLine : line_id, plant_name, department_name, line_name
  DownTime       : id, line_id, downtime_date, downtime_minutes, downtime_reason

Run locally:
    pip install -r requirements.txt
    uvicorn agent:app --host 0.0.0.0 --port 8000 --reload

Environment variables (create a .env file):
    DB_SERVER=your_server
    DB_NAME=your_database
    DB_USER=your_user
    DB_PASSWORD=your_password
    AWS_REGION=us-east-1
    BEDROCK_MODEL_ID=amazon.nova-pro-v1:0
"""

import json
import os
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional

import pyodbc
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from strands import Agent, tool
from strands.models import BedrockModel

from tools import (
    get_all_production_lines,
    get_total_downtime_per_line,
    get_daily_downtime, 
    compare_lines,
    get_downtime_by_reason,
    get_department_summary,
)
from db_handler import (get_connection, run_query)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_SERVER   = os.getenv("DB_SERVER", "localhost")
DB_NAME     = os.getenv("DB_NAME", "ManufacturingDB")
DB_USER     = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")
MODEL_ID    = os.getenv("BEDROCK_MODEL_ID", "amazon.nova-pro-v1:0")

# ── In-memory session store ───────────────────────────────────────────────────
# Stores conversation history per session_id so the agent remembers context.
sessions: dict[str, list[dict]] = {}

# ── Database helper ───────────────────────────────────────────────────────────



# ── Build agent ───────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
You are a Manufacturing Operations Intelligence assistant connected to a MSSQL database.

Database schema:
  ProductionLine: line_id, plant_name, department_name, line_name
    - plant_name: Plant1
    - department_name: Alpha, Beta
    - line_name: Alpha1, Alpha2, Alpha3, Beta1, Beta2, Beta3, Beta4

  DownTime: id, line_id, downtime_date, downtime_minutes, downtime_reason
    - Some rows have downtime_minutes=0 and NULL downtime_reason (no-downtime records — ignore these)
    - downtime_reason values: Material Shortage, Mechanical Failure, Scheduled Maintenance,
      Power Outage, Operator Break, Quality Check Stop

Rules:
- Always call a tool to fetch fresh data before answering.
- Exclude rows where downtime_minutes = 0 or downtime_reason IS NULL from analysis.
- When comparing departments, use get_department_summary.
- When asked for a chart or plot, describe the data clearly with numbers — the frontend renders visuals.
- Keep answers concise and data-driven. Include actual numbers.
- Default time window is 7 days unless the user specifies otherwise.
""".strip()


def build_agent(model_id: str) -> Agent:
    return Agent(
        model=BedrockModel(model_id=model_id, region_name=AWS_REGION),
        tools=[
            get_all_production_lines,
            get_total_downtime_per_line,
            get_daily_downtime,
            compare_lines,
            get_downtime_by_reason,
            get_department_summary,
        ],
        system_prompt=SYSTEM_PROMPT,
    )

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="MFG Intelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Request / Response models ─────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    model_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    session_id: str
    tool_calls: list[str] = []

class NewSessionResponse(BaseModel):
    session_id: str

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.post("/session/new", response_model=NewSessionResponse)
def new_session():
    """Create a new empty session and return its ID."""
    sid = str(uuid.uuid4())
    sessions[sid] = []
    logger.info("New session: %s", sid)
    return {"session_id": sid}


@app.delete("/session/{session_id}")
def delete_session(session_id: str):
    """Clear and delete a session."""
    sessions.pop(session_id, None)
    return {"deleted": session_id}


@app.get("/session/{session_id}/history")
def get_history(session_id: str):
    """Return the conversation history for a session."""
    return {"session_id": session_id, "history": sessions.get(session_id, [])}


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    """
    Main chat endpoint. Accepts a message + optional session_id.
    Creates a new session automatically if none provided.
    """
    session_id = req.session_id or str(uuid.uuid4())
    if session_id not in sessions:
        sessions[session_id] = []

    model_id = req.model_id or MODEL_ID
    logger.info("Session=%s Model=%s Msg='%s'", session_id, model_id, req.message)

    # Build conversation history string for context injection
    history = sessions[session_id]
    context_prefix = ""
    if history:
        context_prefix = "Previous conversation:\n"
        for turn in history[-6:]:          # last 3 exchanges = 6 turns
            role = "User" if turn["role"] == "user" else "Assistant"
            context_prefix += f"{role}: {turn['content']}\n"
        context_prefix += "\nCurrent question: "

    full_message = context_prefix + req.message

    # Track which tools were called
    tool_calls_made: list[str] = []
    original_tools = [
        get_all_production_lines,
        get_total_downtime_per_line,
        get_daily_downtime,
        compare_lines,
        get_downtime_by_reason,
        get_department_summary,
    ]

    try:
        agent = build_agent(model_id)
        result = agent(full_message)
        response_text = str(result)
    except Exception as exc:
        logger.exception("Agent error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    # Save turns to session history
    sessions[session_id].append({"role": "user",      "content": req.message})
    sessions[session_id].append({"role": "assistant",  "content": response_text})

    return ChatResponse(
        response=response_text,
        session_id=session_id,
        tool_calls=tool_calls_made,
    )


@app.get("/lines")
def list_lines():
    """Quick endpoint to list all production lines — used by the frontend dropdown."""
    try:
        rows = run_query("SELECT line_id, plant_name, department_name, line_name FROM ProductionLine ORDER BY line_name")
        return {"lines": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("agent:app", host="0.0.0.0", port=8000, reload=True)