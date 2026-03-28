# MCPGate Orchestration Backend

This project contains the robust backend for the MCPGate dashboard built with **FastAPI**, **Redis**, and **PostgreSQL**.

It handles:
1. **DAG Workflow Execution**: Uses Python's `asyncio.gather()` to evaluate topological layers in parallel.
2. **Context Store**: A Redis hash store containing execution context linked by `execution_id`. 
3. **Audit Trail**: Every node execution step natively writes log events (Start, Success, Approval Gates, etc.) into PostgreSQL.
4. **Approval Gates**: A simulation of pausing the `asyncio` loop until manual administrative intervention resolves a pause token (useful for the `write:branches` flow).

## Instructions for setting up

1. **Start required services (Redis, PostgreSQL)**
   ```shell
   docker-compose up -d
   ```

2. **Install dependencies**
   ```shell
   pip install -r requirements.txt
   ```

3. **Start the FastAPI server**
   ```shell
   python main.py
   ```
   > Server will run at `http://localhost:8000`

## Integration with Dashboard
To fully link `mcpgate.html` to the live backend:
Replace `executeWorkflow()` simulation logic in `.html` to fire an HTTP POST request to `/api/v1/execute` wrapping the Workflow DAG schema. Then poll `/api/v1/status/{execution_id}` for runtime updates.
