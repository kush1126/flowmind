import asyncio
import uuid
import json
import os
import sqlite3
import httpx
import base64
from datetime import datetime
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import uvicorn

load_dotenv()

# --- CONFIG & DEFAULTS ---
DB_FILE = "flowmind_structured.db"
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2")

# --- DATABASE SCHEMAS (SQLite Version) ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS login_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_email TEXT NOT NULL,
    ip_address TEXT,
    login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'SUCCESS'
);

CREATE TABLE IF NOT EXISTS llm_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    workflow_name TEXT,
    prompt TEXT,
    plan_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    workflow_name TEXT,
    status TEXT DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ISOLATED LOG TABLES
CREATE TABLE IF NOT EXISTS github_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    node_id TEXT,
    action TEXT,
    status TEXT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS slack_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    node_id TEXT,
    action TEXT,
    status TEXT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jira_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    node_id TEXT,
    action TEXT,
    status TEXT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sheets_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    node_id TEXT,
    action TEXT,
    status TEXT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS general_audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    node_id TEXT,
    service TEXT,
    action TEXT,
    status TEXT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# --- IN-MEMORY STATE (Redis Fallback) ---
runtime_state = {}

# --- MODELS ---
class Node(BaseModel):
    id: str
    label: str
    sub: Optional[str] = "None"
    svc: str
    gate: bool = False
    details: Optional[str] = None
    x: Optional[float] = 0
    y: Optional[float] = 0
    w: Optional[float] = 110
    h: Optional[float] = 44

class Edge(BaseModel):
    f: str
    t: str

class WorkflowPayload(BaseModel):
    name: str
    nodes: List[Node]
    edges: List[Edge]

class PlanRequest(BaseModel):
    prompt: str

class LoginPayload(BaseModel):
    email: str

# --- DB UTILS ---
def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript(CREATE_TABLES_SQL)
        print("SQLite (Lite Mode) Schemas Initialized.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield
    print("Cleanup: Done.")

app = FastAPI(title="Flowmind — Standalone Agentic Gateway", lifespan=lifespan)

# --- UTILS ---
def topo_sort(nodes: List[Node], edges: List[Edge]) -> List[List[str]]:
    in_deg = {n.id: 0 for n in nodes}
    adj = {n.id: [] for n in nodes}
    for e in edges:
        adj[e.f].append(e.t)
        in_deg[e.t] += 1
    result = []
    if not edges: return [[n.id for n in nodes]]
    queue = [n.id for n in nodes if in_deg[n.id] == 0]
    while queue:
        result.append(list(queue))
        next_q = []
        for curr in queue:
            for neighbor in adj.get(curr, []):
                in_deg[neighbor] -= 1
                if in_deg[neighbor] == 0:
                    next_q.append(neighbor)
        queue = next_q
    return result

async def log_audit(execution_id: str, node_id: str, svc: str, action: str, status: str, payload: dict = None):
    svc_lower = svc.lower()
    table_map = {
        "github": "github_actions",
        "jira": "jira_actions",
        "slack": "slack_actions",
        "sheets": "sheets_actions"
    }
    target_table = table_map.get(svc_lower, "general_audit_logs")
    payload_json = json.dumps(payload) if payload else None
    
    with get_db() as conn:
        if target_table == "general_audit_logs":
            conn.execute(
                "INSERT INTO general_audit_logs (execution_id, node_id, service, action, status, payload) VALUES (?, ?, ?, ?, ?, ?)",
                (execution_id, node_id, svc, action, status, payload_json)
            )
        else:
            conn.execute(
                f"INSERT INTO {target_table} (execution_id, node_id, action, status, payload) VALUES (?, ?, ?, ?, ?)",
                (execution_id, node_id, action, status, payload_json)
            )
    print(f"AUDIT LOG | {svc} -> {status} (Saved to {target_table})")

async def set_exec_status(execution_id: str, status: str, data: dict = None):
    if execution_id not in runtime_state:
        runtime_state[execution_id] = {"id": execution_id}
    runtime_state[execution_id].update({"status": status})
    if data:
        runtime_state[execution_id].update(data)
    print(f"STATE | {execution_id} -> {status}")

# --- DAG ORCHESTRATION ---
# --- JIRA INTEGRATION HELPERS ---
async def create_jira_issue(summary: str, description: str = "Created via Flowmind Agentic Gateway"):
    token = os.getenv("JIRA_API_TOKEN")
    domain = os.getenv("JIRA_DOMAIN")
    user = os.getenv("JIRA_USER_EMAIL")
    project = os.getenv("JIRA_PROJECT_KEY", "KAN")
    
    if not all([token, domain, user]):
        return {"error": "Jira credentials missing in .env"}

    url = f"https://{domain}/rest/api/3/issue"
    auth_str = f"{user}:{token}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_b64}"
    }
    
    payload = {
        "fields": {
            "project": {"key": project},
            "summary": summary,
            "description": {
                "type": "doc",
                "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}]
            },
            "issuetype": {"name": "Task"}
        }
    }
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(url, json=payload, headers=headers, timeout=20.0)
            resp.raise_for_status()
            data = resp.json()
            return {"key": data.get("key"), "id": data.get("id"), "url": f"https://{domain}/browse/{data.get('key')}"}
        except Exception as e:
            print(f"Jira API Error: {e}")
            return {"error": str(e)}

async def complete_all_jira_tasks():
    token = os.getenv("JIRA_API_TOKEN")
    domain = os.getenv("JIRA_DOMAIN")
    user = os.getenv("JIRA_USER_EMAIL")
    project = os.getenv("JIRA_PROJECT_KEY", "KAN")
    
    if not all([token, domain, user]):
        return {"error": "Jira credentials missing in .env"}
        
    auth_str = f"{user}:{token}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()
    headers = {"Accept": "application/json", "Authorization": f"Basic {auth_b64}"}
    
    async with httpx.AsyncClient() as client:
        try:
            jql = f"project={project} AND statusCategory != Done"
            search_url = f"https://{domain}/rest/api/3/search"
            resp = await client.get(search_url, params={"jql": jql}, headers=headers, timeout=20.0)
            resp.raise_for_status()
            
            issues = resp.json().get("issues", [])
            completed_count = 0
            
            for issue in issues:
                key = issue["key"]
                trans_url = f"https://{domain}/rest/api/3/issue/{key}/transitions"
                t_resp = await client.get(trans_url, headers=headers, timeout=10.0)
                if t_resp.status_code != 200:
                    continue
                
                transitions = t_resp.json().get("transitions", [])
                done_trans = next((t for t in transitions if "done" in t["name"].lower() or "close" in t["name"].lower() or "complete" in t["name"].lower()), None)
                
                if done_trans:
                    post_headers = {**headers, "Content-Type": "application/json"}
                    p_resp = await client.post(trans_url, json={"transition": {"id": done_trans["id"]}}, headers=post_headers, timeout=10.0)
                    if p_resp.status_code in (200, 204):
                        completed_count += 1
                        
            return {"message": f"Successfully completed {completed_count} tasks", "completed": completed_count}
        except Exception as e:
            print(f"Jira Completion Error: {e}")
            return {"error": str(e)}

async def delete_jira_issue(issue_key: str):
    token = os.getenv("JIRA_API_TOKEN")
    domain = os.getenv("JIRA_DOMAIN")
    user = os.getenv("JIRA_USER_EMAIL")
    
    auth_b64 = base64.b64encode(f"{user}:{token}".encode()).decode()
    url = f"https://{domain}/rest/api/3/issue/{issue_key}"
    headers = {"Authorization": f"Basic {auth_b64}"}
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.delete(url, headers=headers, timeout=10.0)
            if resp.status_code in (200, 204):
                return {"message": f"Deleted {issue_key}"}
            return {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

async def add_jira_label(issue_key: str, label: str):
    token = os.getenv("JIRA_API_TOKEN")
    domain = os.getenv("JIRA_DOMAIN")
    user = os.getenv("JIRA_USER_EMAIL")
    
    auth_b64 = base64.b64encode(f"{user}:{token}".encode()).decode()
    url = f"https://{domain}/rest/api/3/issue/{issue_key}"
    headers = {"Authorization": f"Basic {auth_b64}", "Content-Type": "application/json"}
    payload = {"update": {"labels": [{"add": label}]}}
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.put(url, json=payload, headers=headers, timeout=10.0)
            if resp.status_code in (200, 204):
                return {"message": f"Added label {label} to {issue_key}"}
            return {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

async def link_jira_issues(inward: str, outward: str):
    token = os.getenv("JIRA_API_TOKEN")
    domain = os.getenv("JIRA_DOMAIN")
    user = os.getenv("JIRA_USER_EMAIL")
    
    auth_b64 = base64.b64encode(f"{user}:{token}".encode()).decode()
    url = f"https://{domain}/rest/api/3/issueLink"
    headers = {"Authorization": f"Basic {auth_b64}", "Content-Type": "application/json"}
    payload = {"type": {"name": "Relates"}, "inwardIssue": {"key": inward}, "outwardIssue": {"key": outward}}
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(url, json=payload, headers=headers, timeout=10.0)
            if resp.status_code in (200, 201, 204):
                return {"message": f"Linked {inward} to {outward}"}
            return {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

async def transition_jira_issue_status(issue_key: str, target_status: str):
    token = os.getenv("JIRA_API_TOKEN")
    domain = os.getenv("JIRA_DOMAIN")
    user = os.getenv("JIRA_USER_EMAIL")
    
    auth_b64 = base64.b64encode(f"{user}:{token}".encode()).decode()
    url = f"https://{domain}/rest/api/3/issue/{issue_key}/transitions"
    headers = {"Authorization": f"Basic {auth_b64}", "Content-Type": "application/json"}
    
    async with httpx.AsyncClient() as client:
        try:
            t_resp = await client.get(url, headers=headers, timeout=10.0)
            if t_resp.status_code != 200:
                return {"error": "Failed to fetch transitions"}
            transitions = t_resp.json().get("transitions", [])
            target = next((t for t in transitions if target_status.lower() in t["name"].lower()), None)
            
            if target:
                p_resp = await client.post(url, json={"transition": {"id": target["id"]}}, headers=headers, timeout=10.0)
                if p_resp.status_code in (200, 204):
                    return {"message": f"Transitioned {issue_key} to {target['name']}"}
                return {"error": p_resp.text}
            return {"error": f"Status '{target_status}' not found"}
        except Exception as e:
            return {"error": str(e)}

async def send_slack_message(channel: str, text: str):
    token = os.getenv("SLACK_API_TOKEN")
    if not token: return {"error": "SLACK_API_TOKEN not found in .env"}
        
    url = "https://slack.com/api/chat.postMessage"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"channel": channel, "text": text}
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(url, json=payload, headers=headers, timeout=10.0)
            data = resp.json()
            if data.get("ok"): return {"message": f"Message sent to {channel}"}
            return {"error": data.get("error", "Unknown Slack error")}
        except Exception as e:
            return {"error": str(e)}

async def create_slack_channel(channel_name: str):
    token = os.getenv("SLACK_API_TOKEN")
    if not token: return {"error": "SLACK_API_TOKEN not found in .env"}
        
    url = "https://slack.com/api/conversations.create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # Slack channel names must be lowercase and have no spaces
    safe_name = channel_name.lower().replace(" ", "-").replace("#", "")
    payload = {"name": safe_name}
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(url, json=payload, headers=headers, timeout=10.0)
            data = resp.json()
            if data.get("ok"): return {"message": f"Channel #{safe_name} created!"}
            return {"error": data.get("error", "Unknown Slack error")}
        except Exception as e:
            return {"error": str(e)}

async def append_google_sheet(spreadsheet_id: str, row_values: list):
    if not spreadsheet_id: return {"error": "No Google Sheet ID provided via ENV"}
    
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request
    except ImportError:
        return {"error": "google-auth not installed in backend instance"}
        
    creds_file = "arctic-dynamo-467807-j8-e1eb7a81e323.json"
    if not os.path.exists(creds_file): return {"error": f"Credentials file {creds_file} missing"}
        
    try:
        creds = service_account.Credentials.from_service_account_file(
            creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        creds.refresh(Request())
        access_token = creds.token
    except Exception as e:
        return {"error": f"Auth failed or file format invalid: {e}"}

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/A1:append?valueInputOption=USER_ENTERED"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    payload = {"values": [row_values]}
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(url, json=payload, headers=headers, timeout=10.0)
            data = resp.json()
            if "updates" in data: return {"message": f"Appended {len(row_values)} columns to sheet successfully"}
            return {"error": data.get("error", {}).get("message", "Unknown Sheets error")}
        except Exception as e:
            return {"error": str(e)}

async def execute_node(execution_id: str, node: Node):
    await set_exec_status(execution_id, "RUNNING", {"current_step": node.label})
    await log_audit(execution_id, node.id, node.svc, node.sub, "RUNNING")
    
    svc_upper = (node.svc or "").upper()
    label_upper = (node.label or "").upper()
    print(f"DEBUG: Executing node {node.id} | Svc: {node.svc} | Label: {node.label}")
    
    # Real Integration Branch (Case insensitive)
    if svc_upper == "JIRA" or "JIRA" in label_upper:
        sub_upper = (node.sub or "").upper()
        details_lower = (node.details or "").strip().lower()
        key_guess = node.details.strip().upper() if node.details else ""
        
        if "DELETE" in sub_upper:
            print(f"Executing REAL Jira call to DELETE: {key_guess}")
            result = await delete_jira_issue(key_guess)
        elif "LABEL" in sub_upper:
            print(f"Executing REAL Jira call to LABEL: {key_guess}")
            result = await add_jira_label(key_guess, "Flowmind-Generated")
        elif "LINK" in sub_upper:
            # Assumes 'KAN-1, KAN-2' format in details
            keys = [k.strip() for k in key_guess.split(",") if k.strip()]
            if len(keys) >= 2:
                print(f"Executing REAL Jira call to LINK: {keys[0]} & {keys[1]}")
                result = await link_jira_issues(keys[0], keys[1])
            else:
                result = {"error": "Need two issue keys separated by comma to link"}
        elif "STATUS" in sub_upper or "MOVE" in sub_upper or "TRANSITION" in sub_upper:
            print(f"Executing REAL Jira call to TRANSITION: {key_guess}")
            result = await transition_jira_issue_status(key_guess, label_upper)
        elif "COMPLETE" in label_upper or "COMPLETE" in sub_upper or "DONE" in sub_upper:
            print(f"Executing REAL Jira call to complete tasks for: {node.label}")
            result = await complete_all_jira_tasks()
        else:
            print(f"Executing REAL Jira call to CREATE for: {node.label}")
            result = await create_jira_issue(node.label, node.details or "Flowmind automated task.")
            
        if "error" in result:
            await log_audit(execution_id, node.id, node.svc, node.sub, "FAILED", result)
            # Fail gracefully, don't crash orchestrator
            return node.id
        await log_audit(execution_id, node.id, node.svc, node.sub, "COMPLETED", result)
        return node.id
        
    elif svc_upper == "SLACK" or "SLACK" in label_upper:
        sub_upper = (node.sub or "").upper()
        
        if "CHANNEL" in sub_upper:
            print(f"Executing REAL Slack call to CREATE CHANNEL: {node.label}")
            result = await create_slack_channel(node.label or "new-channel")
        else:
            print(f"Executing REAL Slack call for MESSAGE: {node.label}")
            channel = node.details.strip() if node.details else os.getenv("SLACK_DEFAULT_CHANNEL", "#general")
            # Auto format the channel string to be Slack compliant if it isn't an ID
            if not channel.startswith("#") and not channel.startswith("C"):
                channel = f"#{channel.lower().replace(' ', '-')}"
                
            result = await send_slack_message(channel, node.label or "Flowmind Automated Message")
        
        if "error" in result:
            await log_audit(execution_id, node.id, node.svc, node.sub, "FAILED", result)
            return node.id
        await log_audit(execution_id, node.id, node.svc, node.sub, "COMPLETED", result)
        return node.id
        
    elif svc_upper == "SHEETS" or "SHEET" in svc_upper or "SHEET" in label_upper:
        print(f"Executing REAL Sheets call for: {node.label}")
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        values = [node.label, node.details or "Flowmind Extracted Entity", "Agent Orchestration"]
        result = await append_google_sheet(sheet_id, values)
        
        if "error" in result:
            await log_audit(execution_id, node.id, node.svc, node.sub, "FAILED", result)
            return node.id
        await log_audit(execution_id, node.id, node.svc, node.sub, "COMPLETED", result)
        return node.id
    
    # Other services (Mocked for now)
    await asyncio.sleep(1) 
    print(f"Mocking service: {node.svc}")
    await log_audit(execution_id, node.id, node.svc, node.sub, "COMPLETED")
    return node.id

async def run_dag(execution_id: str, payload: WorkflowPayload):
    try:
        layers = topo_sort(payload.nodes, payload.edges)
        print(f"ORCHESTRATOR | Start Execution: {execution_id}")
        await set_exec_status(execution_id, "RUNNING", {"total_layers": len(layers)})
        await log_audit(execution_id, "SYSTEM", "Orchestrator", "DAG_START", "STARTED")
        
        for layerNum, layer in enumerate(layers):
            tasks = []
            for node_id in layer:
                try:
                    node = next(n for n in payload.nodes if n.id == node_id)
                    tasks.append(execute_node(execution_id, node))
                except Exception as node_e:
                    print(f"Node Resolve Error: {node_e}")
            
            if tasks:
                await asyncio.gather(*tasks)
            
        await set_exec_status(execution_id, "COMPLETED")
        await log_audit(execution_id, "SYSTEM", "Orchestrator", "DAG_FINISH", "SUCCESS")
        
        with get_db() as conn:
            conn.execute("UPDATE executions SET status = 'COMPLETED' WHERE id = ?", (execution_id,))
            
    except Exception as e:
        await set_exec_status(execution_id, "FAILED", {"error": str(e)})
        await log_audit(execution_id, "SYSTEM", "Orchestrator", "DAG_FAILURE", "FAILED", {"error": str(e)})

# --- ROUTES ---

@app.get("/api/v1/health")
async def health_check():
    ollama_status = "OFFLINE"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=3.0)
            if resp.status_code == 200: 
                ollama_status = "ONLINE"
    except Exception as e:
        print(f"Health Check Diagnostic: {e}")
    return {"status": "ok", "ollama": ollama_status, "storage": "SQLite"}

@app.get("/")
async def serve_frontend():
    return FileResponse("mcpgate.html")

@app.post("/api/v1/auth/login")
async def register_login(payload: LoginPayload, request: Request):
    ip = request.client.host if request.client else "unknown"
    with get_db() as conn:
        conn.execute("INSERT INTO login_sessions (user_email, ip_address) VALUES (?, ?)", (payload.email, ip))
        # Upsert user
        conn.execute("""
            INSERT INTO users (id, email, last_login) VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (email) DO UPDATE SET last_login = EXCLUDED.last_login
        """, (str(uuid.uuid4()), payload.email))
    return {"status": "ok"}

@app.post("/api/v1/plan")
async def plan_workflow(req: PlanRequest):
    execution_id = str(uuid.uuid4())
    
    # SYSTEM PROMPT
    system_prompt = f"""You are a DAG planner for Flowmind. Decompose user request into a JSON object.
Output MUST be an object with these exact keys: "name", "nodes", "edges".
Nodes must be an array of {{id, label, sub, svc, gate, details, x, y, w, h}}.

CRITICAL JIRA RULES:
- Use svc: "Jira" for ANY Jira actions.
1. To CREATE an issue: set "sub": "CREATE", "label": <Title>, "details": <Description>.
2. To CHANGE STATUS/MOVE: set "sub": "STATUS", "label": <Target Status exactly, e.g. 'In Progress'>, "details": <Issue Key e.g. 'KAN-4'>.
3. To DELETE: set "sub": "DELETE", "details": <Issue Key>.
4. To ADD LABEL: set "sub": "LABEL", "label": <Label name>, "details": <Issue Key>.
5. To LINK: set "sub": "LINK", "details": <Issue 1, Issue 2>.
6. To COMPLETE ALL: set "sub": "COMPLETE".

CRITICAL SLACK RULES:
- Use svc: "Slack" for ANY Slack messages or channels.
  - To SEND MESSAGE: set "sub": "MESSAGE", "label": <Message Content>, "details": <Channel Name/ID>.
  - To CREATE CHANNEL: set "sub": "CHANNEL", "label": <New Channel Name>.

CRITICAL SHEETS RULES:
- Use svc: "Sheets" for ANY Google Sheets operations.
  - Set "sub": "CREATE", "label": <Data Record 1>, "details": <Data Record 2>.

EXAMPLE INPUT: "move kan4 to in progress and send a message [hello] in slack channel #social"
EXAMPLE OUTPUT:
{{
  "name": "Update and Notify",
  "nodes": [
    {{ "id": "1", "label": "In Progress", "sub": "STATUS", "svc": "Jira", "gate": false, "details": "KAN-4" }},
    {{ "id": "2", "label": "hello", "sub": "MESSAGE", "svc": "Slack", "gate": false, "details": "social" }}
  ],
  "edges": []
}}

Return ONLY the JSON object. No markdown."""

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": LLM_MODEL,
                    "prompt": f"{system_prompt}\nUser Prompt: {req.prompt}",
                    "stream": False,
                    "format": "json"
                },
                timeout=45.0
            )
            resp.raise_for_status()
            llm_output = resp.json().get("response", "{}")
            dag_data = json.loads(llm_output)
            
            # Ensure keys exist
            if "name" not in dag_data: dag_data["name"] = "Generated Workflow"
            if "nodes" not in dag_data: dag_data["nodes"] = []
            if "edges" not in dag_data: dag_data["edges"] = []
            
            # Auto-layout and fix missing labels
            for i, n in enumerate(dag_data.get("nodes", [])):
                if "x" not in n: n["x"] = 20 + (i * 150)
                if "y" not in n: n["y"] = 118
                if "w" not in n: n["w"] = 110
                if "h" not in n: n["h"] = 44
                if not n.get("label"): n["label"] = f"Step {i+1}"
                if not n.get("svc"): n["svc"] = "Internal"
                if not n.get("id"): n["id"] = f"node_{i}"
                n["id"] = str(n["id"])
                
            for e in dag_data.get("edges", []):
                if "f" in e: e["f"] = str(e["f"])
                if "t" in e: e["t"] = str(e["t"])
            
            # Store Plan
            with get_db() as conn:
                conn.execute(
                    "INSERT INTO executions (id, workflow_name) VALUES (?, ?)",
                    (execution_id, dag_data.get("name", "Generated Workflow"))
                )
                conn.execute(
                    "INSERT INTO llm_plans (execution_id, workflow_name, prompt, plan_json) VALUES (?, ?, ?, ?)",
                    (execution_id, dag_data.get("name", "Generated Workflow"), req.prompt, json.dumps(dag_data))
                )
            
            # Initialize State Context
            runtime_state[execution_id] = {
                "id": execution_id,
                "status": "READY",
                "dag": dag_data
            }
            
            return {"execution_id": execution_id, "dag": dag_data}
            
    except Exception as e:
        print(f"Ollama Plan Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/execute/{execution_id}")
async def start_execution(execution_id: str, background_tasks: BackgroundTasks):
    state_entry = runtime_state.get(execution_id)
    if not state_entry:
        raise HTTPException(status_code=404, detail="Plan not found")
        
    payload = WorkflowPayload(**state_entry["dag"])
    background_tasks.add_task(run_dag, execution_id, payload)
    return {"status": "dispatched"}

@app.get("/api/v1/status/{execution_id}")
async def get_status(execution_id: str):
    data = runtime_state.get(execution_id)
    if not data:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"execution_id": execution_id, "data": data}

@app.get("/api/v1/metrics")
async def get_metrics():
    with get_db() as conn:
        counts = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        nodes = conn.execute("SELECT COUNT(*) FROM llm_plans").fetchone()[0] # Rough Node count proxy
        return {"total_runs": counts, "total_plans": nodes}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
