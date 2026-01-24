# apps/api/src/main.py

from fastapi import FastAPI

from db.session import engine
from db.models import Base

from api.v1.health import router as health_router
from api.v1.search import router as search_router

from api.v1.search_history import router as search_history_router


app = FastAPI(
    title="Auto Semantic Search MVP",
    version="0.1.0",
)

app.include_router(health_router, prefix="/api/v1")
app.include_router(search_router, prefix="/api/v1")
app.include_router(search_history_router, prefix="/api/v1")

Base.metadata.create_all(bind=engine)
