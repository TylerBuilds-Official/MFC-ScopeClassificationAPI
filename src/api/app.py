"""FastAPI application factory with lifespan-managed engine singleton."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from scope_classification import ScopeAnalysisEngine

from .job_runner import JobRunner
from .routers import sessions, matches, categories, exclusions, analyze, action_items, training, user_info, admin


@asynccontextmanager
async def lifespan(application: FastAPI):
    """

    Spin up the engine once, tear down on shutdown.
    """

    env_path = str(Path(__file__).resolve().parents[2] / ".env")
    engine   = ScopeAnalysisEngine.from_env(env_path)

    application.state.engine     = engine
    application.state.job_runner = JobRunner()

    yield

    # Teardown — close DB pool
    engine._db.close()


app = FastAPI(
    title       = "Scope Classification API",
    description = "REST bridge for the ScopeClassificationEngine",
    version     = "0.1.0",
    lifespan    = lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins   = [
        "https://10.0.0.12:7002",
        "https://localhost:5173",
        "https://127.0.0.1:5173",
    ],
    allow_methods   = ["*"],
    allow_headers   = ["*"],
    allow_credentials = True,
)

# ── Routers ──────────────────────────────────────────────────────────
app.include_router(sessions.router,   prefix="/api/sessions",   tags=["Sessions"])
app.include_router(matches.router,    prefix="/api/matches",    tags=["Matches"])
app.include_router(categories.router, prefix="/api/categories", tags=["Categories"])
app.include_router(exclusions.router, prefix="/api/exclusions/mfc", tags=["MFC Exclusions"])
app.include_router(analyze.router,       prefix="/api/analyze",       tags=["Analyze"])
app.include_router(action_items.router, prefix="/api/action-items", tags=["Action Items"])
app.include_router(training.router,     prefix="/api/training",     tags=["Training"])
app.include_router(user_info.router,    prefix="/api",              tags=["Auth"])
app.include_router(admin.router,        prefix="/api/admin",        tags=["Admin"])


@app.get("/api/health")
async def health() -> dict:
    """Quick health check."""

    return {"status": "ok"}
