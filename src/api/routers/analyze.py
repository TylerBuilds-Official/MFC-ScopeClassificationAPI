"""Analyze endpoint — launch the engine pipeline in the background."""

import logging
import shutil
import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from scope_classification import ScopeAnalysisEngine, SessionRepo

from ..dependencies import get_engine, get_job_runner, get_session_repo
from ..job_runner import JobRunner


router = APIRouter()
log     = logging.getLogger(__name__)


@router.post("")
async def analyze_scope_letter(
        network_path: str | None        = Form(None),
        erector_name: str | None        = Form(None),
        job_number:   str | None        = Form(None),
        job_name:     str | None        = Form(None),
        initiated_by: str | None        = Form(None),
        archive:      bool              = Form(True),
        file:         UploadFile | None = File(None),
        engine: ScopeAnalysisEngine     = Depends(get_engine),
        runner: JobRunner               = Depends(get_job_runner) ) -> dict:
    """

    Launch the full extract → classify → compare pipeline in the background.

    Returns immediately with session_id. Poll GET /sessions/{id}/progress
    to track status.
    """

    pdf_path = _resolve_pdf(network_path, file)

    # Pre-create the session so we can return the id immediately.
    # Resolve erector the same way the engine does internally.
    erector_id, resolved_name = engine._resolve_erector(erector_name)
    display_name              = resolved_name or erector_name

    session_id = engine._session_repo.create(
        source_file_name = Path(pdf_path).name,
        erector_id       = erector_id,
        erector_name_raw = display_name,
        job_number       = job_number,
        job_name         = job_name,
        source_file_path = pdf_path,
        initiated_by     = initiated_by,
    )

    # Spawn background thread for the heavy lifting
    runner.submit(
        session_id = session_id,
        target     = _run_pipeline,
        args       = (engine, session_id, pdf_path, display_name, job_number, archive),
    )

    return {
        "session_id": session_id,
        "status":     "Running",
    }


def _run_pipeline(
        engine:       ScopeAnalysisEngine,
        session_id:   int,
        pdf_path:     str,
        erector_name: str | None,
        job_number:   str | None,
        archive:      bool ) -> None:
    """Run extract → classify → compare on a pre-created session. Runs in a background thread."""

    from scope_classification.constants.session_status import SessionStatus
    from .action_items import generate_action_items_for_session

    pdf = Path(pdf_path)

    # ── EXTRACT ──────────────────────────────────────────────────
    try:
        extraction = engine._extract(pdf, session_id)
        log.info(
            f"  Session {session_id} extract: {extraction.total_items} items "
            f"from {extraction.total_sections} sections"
        )
    except Exception as exc:
        engine._session_repo.update_status(session_id, SessionStatus.ERROR, error_message=f"Extract failed: {exc}")
        log.error(f"  Session {session_id} extract FAILED: {exc}")
        _cleanup_temp(pdf_path)

        return

    # ── CLASSIFY ─────────────────────────────────────────────────
    try:
        classification = engine._classifier.classify_session(session_id, erector_name=erector_name)
        log.info(
            f"  Session {session_id} classify: {classification.total_classified}/{classification.total_extracted} "
            f"({classification.avg_confidence:.0%} avg conf)"
        )
    except Exception as exc:
        engine._session_repo.update_status(session_id, SessionStatus.ERROR, error_message=f"Classification failed: {exc}")
        log.error(f"  Session {session_id} classify FAILED: {exc}")
        _cleanup_temp(pdf_path)

        return

    # ── COMPARE ──────────────────────────────────────────────────
    try:
        comparison = engine._matcher.compare_session(session_id, erector_name=erector_name)
        log.info(
            f"  Session {session_id} compare: Aligned={comparison.total_aligned} "
            f"Partial={comparison.total_partial} ErectorOnly={comparison.total_erector_only} "
            f"MfcOnly={comparison.total_mfc_only}"
        )
    except Exception as exc:
        engine._session_repo.update_status(session_id, SessionStatus.ERROR, error_message=f"Comparison failed: {exc}")
        log.error(f"  Session {session_id} compare FAILED: {exc}")
        _cleanup_temp(pdf_path)

        return

    # ── ACTION ITEMS ─────────────────────────────────────────────
    try:
        count = generate_action_items_for_session(engine._db, session_id)
        log.info(f"  Session {session_id} action items: {count} generated")
    except Exception as exc:
        log.warning(f"  Session {session_id} action item generation failed (non-fatal): {exc}")

    # ── ARCHIVE ──────────────────────────────────────────────────
    if archive:
        try:
            engine._archive_source(pdf, session_id, job_number, erector_name)
        except Exception as exc:
            log.warning(f"  Session {session_id} archive failed (non-fatal): {exc}")

    _cleanup_temp(pdf_path)
    log.info(f"  Session {session_id} pipeline complete")


def _cleanup_temp(pdf_path: str) -> None:
    """Remove temp file if it lives in the system temp directory."""

    p = Path(pdf_path)
    if p.parent == Path(tempfile.gettempdir()) and p.exists():
        p.unlink(missing_ok=True)


def _resolve_pdf(
        network_path: str | None,
        upload: UploadFile | None ) -> str:
    """Determine the PDF path. Tries network_path first, falls back to upload."""

    if network_path:
        p = Path(network_path)
        if p.exists() and p.suffix.lower() == ".pdf":

            return str(p)

        raise HTTPException(
            status_code = 400,
            detail      = f"Network path not accessible or not a PDF: {network_path}",
        )

    if upload:
        if not upload.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Uploaded file must be a PDF")

        tmp = Path(tempfile.gettempdir()) / f"scope_{upload.filename}"
        with tmp.open("wb") as f:
            shutil.copyfileobj(upload.file, f)

        return str(tmp)

    raise HTTPException(
        status_code = 400,
        detail      = "Provide either network_path or upload a PDF file",
    )
