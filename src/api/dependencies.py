"""FastAPI dependencies — engine + repo accessors via request.app.state."""

from fastapi import Request

from scope_classification import (
    ScopeAnalysisEngine,
    ConnectionFactory,
    SessionRepo,
    MatchRepo,
    ExclusionRepo,
)

from .job_runner import JobRunner


def get_engine(request: Request) -> ScopeAnalysisEngine:
    """

    Return the lifespan-initialised engine singleton.
    """

    return request.app.state.engine


def get_db(request: Request) -> ConnectionFactory:
    """

    Return the engine's database connection factory.
    """

    return request.app.state.engine._db


def get_session_repo(request: Request) -> SessionRepo:
    """

    Build a SessionRepo from the engine's connection factory.
    """

    return SessionRepo(request.app.state.engine._db)


def get_match_repo(request: Request) -> MatchRepo:
    """

    Build a MatchRepo from the engine's connection factory.
    """

    return MatchRepo(request.app.state.engine._db)


def get_exclusion_repo(request: Request) -> ExclusionRepo:
    """

    Build an ExclusionRepo from the engine's connection factory.
    """

    return ExclusionRepo(request.app.state.engine._db)


def get_job_runner(request: Request) -> JobRunner:
    """Return the lifespan-initialised job runner."""

    return request.app.state.job_runner
