"""ScopeClassificationAPI — FastAPI bridge for the ScopeClassificationEngine."""

import uvicorn


def main() -> None:
    """Launch the API server."""

    uvicorn.run(
        "src.api.app:app",
        host    = "0.0.0.0",
        port    = 8100,
        reload  = True,
        workers = 1,
    )


if __name__ == "__main__":
    main()
