# backend/app/main.py

from fastapi import FastAPI
from backend.app.routers import ingestion  # absolute import

app = FastAPI(title="Supply Chain Alert System API")

# Include routers
app.include_router(ingestion.router, prefix="/api")
