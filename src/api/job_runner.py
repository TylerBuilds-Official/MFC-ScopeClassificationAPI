"""In-process job runner for background analysis tasks."""

import logging
import threading
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class JobInfo:
    """Tracks a running background analysis."""

    session_id: int
    thread:     threading.Thread
    error:      str | None = None


class JobRunner:
    """Manages background analysis threads. One job per session."""

    def __init__(self) -> None:
        """Initialize with empty job registry."""

        self._jobs: dict[int, JobInfo] = {}
        self._lock = threading.Lock()

    def submit(self, session_id: int, target: callable, args: tuple = ()) -> None:
        """Spawn a background thread for the given session."""

        def _wrapper():
            try:
                target(*args)
            except Exception as exc:
                log.error(f"Background job for session {session_id} failed: {exc}")
                with self._lock:
                    if session_id in self._jobs:
                        self._jobs[session_id].error = str(exc)

        thread = threading.Thread(
            target = _wrapper,
            name   = f"analyze-{session_id}",
            daemon = True,
        )

        with self._lock:
            self._jobs[session_id] = JobInfo(session_id=session_id, thread=thread)

        thread.start()
        log.info(f"Background job started for session {session_id}")

    def is_running(self, session_id: int) -> bool:
        """Check if a job is currently running for the given session."""

        with self._lock:
            job = self._jobs.get(session_id)

            if not job:
                return False

            return job.thread.is_alive()

    def get_error(self, session_id: int) -> str | None:
        """Get error message if the background job failed."""

        with self._lock:
            job = self._jobs.get(session_id)

            return job.error if job else None

    def cleanup(self, session_id: int) -> None:
        """Remove a completed job from the registry."""

        with self._lock:
            if session_id in self._jobs and not self._jobs[session_id].thread.is_alive():
                del self._jobs[session_id]

    def active_session_ids(self) -> list[int]:
        """Return list of session IDs with active background jobs."""

        with self._lock:
            return [sid for sid, job in self._jobs.items() if job.thread.is_alive()]
