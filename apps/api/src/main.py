# apps/api/src/main.py

from fastapi import FastAPI

from api.v1.health import router as health_router
from api.v1.search import router as search_router

app = FastAPI(
    title="Auto Semantic Search MVP",
    version="0.1.0",
)

app.include_router(health_router, prefix="/api/v1")
app.include_router(search_router, prefix="/api/v1")
