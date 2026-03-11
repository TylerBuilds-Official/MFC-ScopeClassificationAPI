"""Cross-erector comparison — create, list, detail, add erector, progress."""

import logging
import shutil
import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from scope_classification import ScopeAnalysisEngine, SessionStatus

from ..auth import User, require_role, require_active_user
from ..dependencies import get_comparison_engine, get_engine, get_job_runner, get_db
from ..job_runner import JobRunner
from ..schemas import (
    ComparisonCreateRequest,
    ComparisonAddErectorRequest,
    ComparisonListItem,
    ComparisonListResponse,
)
from ..services.cross_erector import CrossErectorComparisonEngine
from ..services.cross_erector._errors.comparison_error import CrossErectorComparisonError


router = APIRouter()
log    = logging.getLogger(__name__)


# -- Create from existing sessions ----------------------------------------

@router.post("")
async def create_comparison(
        body:       ComparisonCreateRequest,
        comp:       CrossErectorComparisonEngine = Depends(get_comparison_engine),
        runner:     JobRunner                    = Depends(get_job_runner),
        user:       User                         = Depends(require_role("estimator", "admin")) ) -> dict:
    """Create a cross-erector comparison from existing analysis session IDs.

    Returns immediately with comparison_id. Poll progress to track status.
    """

    initiated_by = body.initiated_by or user.display_name

    try:
        comparison_id = comp.create_comparison(
            analysis_session_ids = body.session_ids,
            job_number           = body.job_number,
            job_name             = body.job_name,
            initiated_by         = initiated_by,
        )
    except CrossErectorComparisonError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Run grouping in background
    runner.submit(
        session_id = _comparison_job_key(comparison_id),
        target     = _run_grouping,
        args       = (comp, comparison_id),
    )

    return {"comparison_id": comparison_id, "status": "Running"}


# -- Create from uploaded PDFs --------------------------------------------

@router.post("/upload")
async def create_comparison_from_uploads(
        erector_names: str                   = Form(...),
        job_number:    str | None            = Form(None),
        job_name:      str | None            = Form(None),
        files:         list[UploadFile]      = File(...),
        engine:        ScopeAnalysisEngine   = Depends(get_engine),
        comp:          CrossErectorComparisonEngine = Depends(get_comparison_engine),
        runner:        JobRunner             = Depends(get_job_runner),
        user:          User                  = Depends(require_role("estimator", "admin")) ) -> dict:
    """Upload multiple PDFs to create a comparison. Runs extract+classify per file, then groups.

    erector_names: comma-separated list matching the order of uploaded files.
    """

    names = [n.strip() for n in erector_names.split(",") if n.strip()]

    if len(files) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 scope letter PDFs")

    if len(files) > 15:
        raise HTTPException(status_code=400, detail="Maximum 15 scope letters per comparison")

    if len(names) != len(files):
        raise HTTPException(
            status_code=400,
            detail=f"Got {len(files)} files but {len(names)} erector names — must match",
        )

    # Save uploaded files to temp
    temp_files = []

    for upload in files:
        if not upload.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"File must be PDF: {upload.filename}")

        tmp = Path(tempfile.gettempdir()) / f"scope_cmp_{upload.filename}"
        with tmp.open("wb") as f:
            shutil.copyfileobj(upload.file, f)

        temp_files.append(str(tmp))

    # Create a placeholder comparison session
    db = engine._db
    cursor = db.execute(
        f"""
        INSERT INTO {db.schema}.ComparisonSessions
            (JobNumber, JobName, InitiatedBy, Status, TotalErectors)
        OUTPUT INSERTED.Id
        VALUES (?, ?, ?, 'Running', ?)
        """,
        (job_number, job_name, user.display_name, len(files)),
    )
    comparison_id = cursor.fetchone()[0]
    db.commit()

    # Run the full pipeline in background
    runner.submit(
        session_id = _comparison_job_key(comparison_id),
        target     = _run_upload_pipeline,
        args       = (engine, comp, comparison_id, temp_files, names, job_number, job_name, user.display_name),
    )

    return {"comparison_id": comparison_id, "status": "Running"}


# -- Re-run grouping on existing comparison --------------------------------

@router.post("/{comparison_id}/rerun")
async def rerun_comparison(
        comparison_id: int,
        comp:          CrossErectorComparisonEngine = Depends(get_comparison_engine),
        runner:        JobRunner                    = Depends(get_job_runner),
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Re-run the grouping phase on an existing comparison."""

    cursor = db.execute(
        f"SELECT Id, Status FROM {db.schema}.ComparisonSessions WHERE Id = ?",
        (comparison_id,),
    )
    row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Comparison {comparison_id} not found")

    # Reset status
    db.execute(
        f"""
        UPDATE {db.schema}.ComparisonSessions
        SET Status = 'Running', CurrentPhase = 'Pending', TotalUnified = 0,
            CompletedAt = NULL, ErrorMessage = NULL
        WHERE Id = ?
        """,
        (comparison_id,),
    )
    db.commit()

    runner.submit(
        session_id = _comparison_job_key(comparison_id),
        target     = _run_grouping,
        args       = (comp, comparison_id),
    )

    return {"comparison_id": comparison_id, "status": "Running", "message": "Re-running grouping"}


# -- Add erector to existing comparison -----------------------------------

@router.post("/{comparison_id}/add")
async def add_erector_to_comparison(
        comparison_id: int,
        body:          ComparisonAddErectorRequest,
        comp:          CrossErectorComparisonEngine = Depends(get_comparison_engine),
        runner:        JobRunner                    = Depends(get_job_runner),
        user:          User                         = Depends(require_role("estimator", "admin")) ) -> dict:
    """Add an erector (existing analysis session) to a comparison and re-run grouping."""

    try:
        comp.add_erector(comparison_id, body.analysis_session_id)
    except CrossErectorComparisonError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Re-run grouping in background
    runner.submit(
        session_id = _comparison_job_key(comparison_id),
        target     = _run_grouping,
        args       = (comp, comparison_id),
    )

    return {"comparison_id": comparison_id, "status": "Running", "message": "Erector added, re-grouping"}


# -- Add erector via PDF upload -------------------------------------------

@router.post("/{comparison_id}/add-upload")
async def add_erector_upload(
        comparison_id: int,
        erector_name:  str              = Form(...),
        job_number:    str | None       = Form(None),
        file:          UploadFile       = File(...),
        engine:        ScopeAnalysisEngine        = Depends(get_engine),
        comp:          CrossErectorComparisonEngine = Depends(get_comparison_engine),
        runner:        JobRunner                    = Depends(get_job_runner),
        user:          User                         = Depends(require_role("estimator", "admin")) ) -> dict:
    """Upload a single PDF to add an erector to an existing comparison."""

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF")

    # Verify comparison exists
    db     = engine._db
    cursor = db.execute(
        f"SELECT Id, Status FROM {db.schema}.ComparisonSessions WHERE Id = ?",
        (comparison_id,),
    )
    row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Comparison {comparison_id} not found")

    # Save to temp
    tmp = Path(tempfile.gettempdir()) / f"scope_add_{file.filename}"
    with tmp.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    # Mark as running
    db.execute(
        f"UPDATE {db.schema}.ComparisonSessions SET Status = 'Running' WHERE Id = ?",
        (comparison_id,),
    )
    db.commit()

    # Run extract+classify+add+regroup in background
    runner.submit(
        session_id = _comparison_job_key(comparison_id),
        target     = _run_add_upload_pipeline,
        args       = (engine, comp, comparison_id, str(tmp), erector_name, job_number, user.display_name),
    )

    return {"comparison_id": comparison_id, "status": "Running", "message": "Analyzing and adding erector"}


# -- List comparisons -----------------------------------------------------

@router.get(
    "",
    response_model = ComparisonListResponse,
)
async def list_comparisons(
        limit:  int       = 50,
        offset: int       = 0,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> ComparisonListResponse:
    """List all comparison sessions with erector names."""

    sql = f"""
        SELECT cs.Id, cs.JobNumber, cs.JobName, cs.Status,
               cs.TotalErectors, cs.TotalUnified, cs.InitiatedBy,
               cs.CreatedAt
        FROM {db.schema}.ComparisonSessions cs
        ORDER BY cs.Id DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """

    cursor  = db.execute(sql, (offset, limit))
    columns = [col[0] for col in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

    # Fetch erector names for each comparison
    comparisons = []

    for row in rows:
        cid = row["Id"]

        name_cursor = db.execute(
            f"""
            SELECT ErectorNameRaw
            FROM {db.schema}.ComparisonSessionErectors
            WHERE ComparisonSessionId = ?
            ORDER BY SortOrder
            """,
            (cid,),
        )
        erector_names = [r[0] for r in name_cursor.fetchall() if r[0]]

        comparisons.append(ComparisonListItem(
            id             = cid,
            job_number     = row["JobNumber"],
            job_name       = row["JobName"],
            status         = row["Status"],
            total_erectors = row["TotalErectors"],
            total_unified  = row["TotalUnified"],
            initiated_by   = row["InitiatedBy"],
            created_at     = str(row["CreatedAt"]) if row["CreatedAt"] else None,
            erector_names  = erector_names,
        ))

    return ComparisonListResponse(comparisons=comparisons, count=len(comparisons))


# -- Get full result ------------------------------------------------------

@router.get("/{comparison_id}")
async def get_comparison(
        comparison_id: int,
        comp: CrossErectorComparisonEngine = Depends(get_comparison_engine),
        user: User                         = Depends(require_active_user) ) -> dict:
    """Full comparison result with unified items + coverage matrix."""

    result = comp.get_result(comparison_id)

    if not result:
        raise HTTPException(status_code=404, detail=f"Comparison {comparison_id} not found")

    return result


# -- Progress poll --------------------------------------------------------

@router.get("/{comparison_id}/progress")
async def get_comparison_progress(
        comparison_id: int,
        runner: JobRunner = Depends(get_job_runner),
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Lightweight progress check for a running comparison."""

    cursor = db.execute(
        f"""
        SELECT Id, Status, TotalErectors, TotalUnified, ErrorMessage, CreatedAt,
               CurrentPhase, ErectorsAnalyzed
        FROM {db.schema}.ComparisonSessions
        WHERE Id = ?
        """,
        (comparison_id,),
    )
    columns = [col[0] for col in cursor.description]
    row     = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Comparison {comparison_id} not found")

    session   = dict(zip(columns, row))
    is_active = runner.is_running(_comparison_job_key(comparison_id))
    error     = runner.get_error(_comparison_job_key(comparison_id))

    if not is_active and error and session["Status"] not in ("Complete", "Error"):
        db.execute(
            f"UPDATE {db.schema}.ComparisonSessions SET Status = 'Error', ErrorMessage = ? WHERE Id = ?",
            (error, comparison_id),
        )
        db.commit()
        session["Status"]       = "Error"
        session["ErrorMessage"] = error

    if not is_active:
        runner.cleanup(_comparison_job_key(comparison_id))

    return {
        "comparison_id":      comparison_id,
        "status":             session["Status"],
        "is_active":          is_active,
        "current_phase":      session["CurrentPhase"],
        "erectors_analyzed":  session["ErectorsAnalyzed"],
        "total_erectors":     session["TotalErectors"],
        "total_unified":      session["TotalUnified"],
        "error_message":      session["ErrorMessage"],
    }


# -- Delete ---------------------------------------------------------------

@router.delete("/{comparison_id}")
async def delete_comparison(
        comparison_id: int,
        db   = Depends(get_db),
        user: User = Depends(require_role("admin")) ) -> dict:
    """Delete a comparison session. Admin only."""

    cursor = db.execute(
        f"SELECT Id FROM {db.schema}.ComparisonSessions WHERE Id = ?",
        (comparison_id,),
    )

    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail=f"Comparison {comparison_id} not found")

    # Delete coverage → unified items → erector links → session
    db.execute(
        f"""
        DELETE FROM {db.schema}.UnifiedItemCoverage
        WHERE UnifiedItemId IN (
            SELECT Id FROM {db.schema}.UnifiedItems WHERE ComparisonSessionId = ?
        )
        """,
        (comparison_id,),
    )
    db.execute(
        f"DELETE FROM {db.schema}.UnifiedItems WHERE ComparisonSessionId = ?",
        (comparison_id,),
    )
    db.execute(
        f"DELETE FROM {db.schema}.ComparisonSessionErectors WHERE ComparisonSessionId = ?",
        (comparison_id,),
    )
    db.execute(
        f"DELETE FROM {db.schema}.ComparisonSessions WHERE Id = ?",
        (comparison_id,),
    )
    db.commit()

    return {"deleted": comparison_id}


# -- Background pipeline functions ----------------------------------------

def _run_grouping(comp: CrossErectorComparisonEngine, comparison_id: int) -> None:
    """Run semantic grouping on an existing ComparisonSession. Background thread target."""

    try:
        comp.run_grouping(comparison_id)
    except Exception as exc:
        log.error(f"Comparison {comparison_id} grouping failed: {exc}")


def _run_upload_pipeline(
        engine:        ScopeAnalysisEngine,
        comp:          CrossErectorComparisonEngine,
        comparison_id: int,
        pdf_paths:     list[str],
        erector_names: list[str],
        job_number:    str | None,
        job_name:      str | None,
        initiated_by:  str ) -> None:
    """Full pipeline: extract+classify each PDF → link to comparison → group. Background thread."""

    db          = engine._db
    session_ids = []

    try:
        total = len(pdf_paths)

        for idx, (pdf_path, erector_name) in enumerate(zip(pdf_paths, erector_names)):
            comp.update_phase(comparison_id, "Analyzing", erectors_analyzed=idx)
            log.info(f"  Comparison {comparison_id}: analyzing {erector_name} ({Path(pdf_path).name}) [{idx + 1}/{total}]")

            # Resolve erector
            erector_id, resolved_name = engine._resolve_erector(erector_name)
            display_name              = resolved_name or erector_name

            # Create analysis session
            session_id = engine._session_repo.create(
                source_file_name = Path(pdf_path).name,
                erector_id       = erector_id,
                erector_name_raw = display_name,
                job_number       = job_number,
                job_name         = job_name,
                source_file_path = pdf_path,
                initiated_by     = initiated_by,
            )

            # Extract
            extraction = engine._extract(Path(pdf_path), session_id)
            log.info(f"    Extract: {extraction.total_items} items")

            # Classify (no MFC comparison needed)
            classification = engine._classifier.classify_session(session_id, erector_name=display_name)
            log.info(f"    Classify: {classification.total_classified}/{classification.total_extracted}")

            # Mark session as classified (not Complete — no MFC compare was done)
            engine._session_repo.update_status(session_id, SessionStatus.CLASSIFIED)

            session_ids.append((session_id, display_name))

            # Cleanup temp file
            _cleanup_temp(pdf_path)

        comp.update_phase(comparison_id, "Linking", erectors_analyzed=len(session_ids))

        # Link all sessions to comparison
        for i, (sid, name) in enumerate(session_ids):
            db.execute(
                f"""
                INSERT INTO {db.schema}.ComparisonSessionErectors
                    (ComparisonSessionId, AnalysisSessionId, ErectorNameRaw, SortOrder)
                VALUES (?, ?, ?, ?)
                """,
                (comparison_id, sid, name, i),
            )

        db.commit()

        # Run cross-erector grouping
        comp.run_grouping(comparison_id)

    except Exception as exc:
        log.error(f"Comparison {comparison_id} upload pipeline failed: {exc}")

        db.execute(
            f"UPDATE {db.schema}.ComparisonSessions SET Status = 'Error', CurrentPhase = 'Error', ErrorMessage = ? WHERE Id = ?",
            (str(exc)[:2000], comparison_id),
        )
        db.commit()

        # Cleanup any remaining temp files
        for pdf_path in pdf_paths:
            _cleanup_temp(pdf_path)


def _run_add_upload_pipeline(
        engine:        ScopeAnalysisEngine,
        comp:          CrossErectorComparisonEngine,
        comparison_id: int,
        pdf_path:      str,
        erector_name:  str,
        job_number:    str | None,
        initiated_by:  str ) -> None:
    """Extract+classify a single PDF, add to comparison, re-group. Background thread."""

    try:
        comp.update_phase(comparison_id, "Analyzing")

        # Resolve erector
        erector_id, resolved_name = engine._resolve_erector(erector_name)
        display_name              = resolved_name or erector_name

        # Create analysis session
        session_id = engine._session_repo.create(
            source_file_name = Path(pdf_path).name,
            erector_id       = erector_id,
            erector_name_raw = display_name,
            job_number       = job_number,
            source_file_path = pdf_path,
            initiated_by     = initiated_by,
        )

        # Extract
        extraction = engine._extract(Path(pdf_path), session_id)
        log.info(f"  Add-upload: {extraction.total_items} items from {display_name}")

        # Classify
        classification = engine._classifier.classify_session(session_id, erector_name=display_name)
        log.info(f"  Add-upload: classified {classification.total_classified}/{classification.total_extracted}")

        engine._session_repo.update_status(session_id, SessionStatus.CLASSIFIED)

        # Add to comparison and re-group
        comp.add_erector(comparison_id, session_id)
        comp.run_grouping(comparison_id)

    except Exception as exc:
        log.error(f"Comparison {comparison_id} add-upload failed: {exc}")

        engine._db.execute(
            f"UPDATE {engine._db.schema}.ComparisonSessions SET Status = 'Error', CurrentPhase = 'Error', ErrorMessage = ? WHERE Id = ?",
            (str(exc)[:2000], comparison_id),
        )
        engine._db.commit()

    finally:
        _cleanup_temp(pdf_path)


def _cleanup_temp(pdf_path: str) -> None:
    """Remove temp file if it lives in the system temp directory."""

    p = Path(pdf_path)
    if p.parent == Path(tempfile.gettempdir()) and p.exists():
        p.unlink(missing_ok=True)


def _comparison_job_key(comparison_id: int) -> int:
    """Generate a unique job key for comparison sessions to avoid collision with analysis session IDs.

    Uses negative IDs so they never collide with AnalysisSession IDs in the job runner.
    """

    return -comparison_id
