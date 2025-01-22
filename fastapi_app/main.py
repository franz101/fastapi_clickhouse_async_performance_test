import asyncio
import logging
import os
import random
import time
from typing import Optional

import httpx
from sqlalchemy import text
import uvicorn
from fastapi import FastAPI, Response,Depends
from opentelemetry.propagate import inject
from utils import PrometheusMiddleware, metrics, setting_otlp
from database import  get_db_async, get_db_sync
from sqlalchemy.ext.asyncio import  AsyncSession


APP_NAME = os.environ.get("APP_NAME", "app")
EXPOSE_PORT = os.environ.get("EXPOSE_PORT", 8000)
OTLP_GRPC_ENDPOINT = os.environ.get("OTLP_GRPC_ENDPOINT", "http://tempo:4317")

TARGET_ONE_HOST = os.environ.get("TARGET_ONE_HOST", "app-b")
TARGET_TWO_HOST = os.environ.get("TARGET_TWO_HOST", "app-c")

app = FastAPI()

# Setting metrics middleware
app.add_middleware(PrometheusMiddleware, app_name=APP_NAME)
app.add_route("/metrics", metrics)

# Setting OpenTelemetry exporter
setting_otlp(app, APP_NAME, OTLP_GRPC_ENDPOINT)


class EndpointFilter(logging.Filter):
    # Uvicorn endpoint access log filter
    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find("GET /metrics") == -1


# Filter out /endpoint
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())


@app.get("/")
async def read_root():
    logging.error("Hello World")
    return {"Hello": "World"}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Optional[str] = None):
    logging.error("items")
    return {"item_id": item_id, "q": q}


@app.get("/io_task")
async def io_task():
    time.sleep(1)
    logging.error("io task")
    return "IO bound task finish!"


@app.get("/cpu_task")
async def cpu_task():
    for i in range(1000):
        _ = i * i * i
    logging.error("cpu task")
    return "CPU bound task finish!"


@app.get("/random_status")
async def random_status(response: Response):
    response.status_code = random.choice([200, 200, 300, 400, 500])
    logging.error("random status")
    return {"path": "/random_status"}


@app.get("/random_sleep")
async def random_sleep(response: Response):
    time.sleep(random.randint(0, 5))
    logging.error("random sleep")
    return {"path": "/random_sleep"}


@app.get("/error_test")
async def error_test(response: Response):
    logging.error("got error!!!!")
    raise ValueError("value error")


@app.get("/chain")
async def chain(response: Response):
    headers = {}
    inject(headers)  # inject trace info to header
    logging.critical(headers)

    async with httpx.AsyncClient() as client:
        await client.get(
            "http://localhost:8000/",
            headers=headers,
        )
    async with httpx.AsyncClient() as client:
        await client.get(
            f"http://{TARGET_ONE_HOST}:8000/io_task",
            headers=headers,
        )
    async with httpx.AsyncClient() as client:
        await client.get(
            f"http://{TARGET_TWO_HOST}:8000/cpu_task",
            headers=headers,
        )
    logging.info("Chain Finished")
    return {"path": "/chain"}


@app.get("/chain_sync")
def chain_sync(response: Response):
    headers = {}
    inject(headers)  # inject trace info to header
    logging.critical(headers)

    with httpx.Client() as client:
        client.get(
            "http://localhost:8000/",
            headers=headers,
        )
    with httpx.Client() as client:
        client.get(
            f"http://{TARGET_ONE_HOST}:8000/io_task",
            headers=headers,
        )
    with httpx.Client() as client:
        client.get(
            f"http://{TARGET_TWO_HOST}:8000/cpu_task",
            headers=headers,
        )
    logging.info("Chain Finished")
    return {"path": "/chain"}



@app.get("/clickhouse_sync")
def clickhouse_sync(response: Response, session = Depends(get_db_sync)):
    headers = {}
    inject(headers)  # inject trace info to header
    logging.critical(headers)
    session.execute(text("SELECT sleep(1)"))
    return {"path": "/clickhouse_sync"}

@app.get("/clickhouse_async")
async def clickhouse_async(response: Response, session: AsyncSession = Depends(get_db_async)):
    headers = {}
    inject(headers)  # inject trace info to header
    logging.critical(headers)
    await session.execute(text("SELECT sleep(1)"))
    return {"path": "/clickhouse_async"}

@app.get("/sleep_sync")
def sleep_sync(response: Response):
    headers = {}
    inject(headers)  # inject trace info to header
    logging.critical(headers)
    time.sleep(1)
    return {"path": "/sleep_sync"}

@app.get("/sleep_async")
async def sleep_sync(response: Response):
    headers = {}
    inject(headers)  # inject trace info to header
    logging.critical(headers)
    asyncio.sleep(1)
    return {"path": "/sleep_async"}

if __name__ == "__main__":
    # update uvicorn access logger format
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"][
        "fmt"
    ] = "%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] [trace_id=%(otelTraceID)s span_id=%(otelSpanID)s resource.service.name=%(otelServiceName)s] - %(message)s"
    uvicorn.run("main:app", host="0.0.0.0", port=EXPOSE_PORT, log_config=log_config, workers=2)
