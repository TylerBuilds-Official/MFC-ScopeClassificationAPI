"""Session endpoints — list and detail views."""

from fastapi import APIRouter, Depends, HTTPException

from scope_classification import SessionRepo, MatchRepo

from ..auth import User, require_active_user, require_role
from ..dependencies import get_session_repo, get_match_repo, get_db, get_job_runner
from ..job_runner import JobRunner
from ..schemas import SessionListItem, SessionListResponse


router = APIRouter()


@router.get(
    "",
    response_model = SessionListResponse,
)
async def list_sessions(
        limit: int  = 50,
        offset: int = 0,
        status: str | None = None,
        repo: SessionRepo  = Depends(get_session_repo),
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> SessionListResponse:
    """

    Paginated session grid with optional status filter.
    """

    where  = "WHERE s.IsDeleted = 0"
    params: list = []

    if status:
        where += " AND s.Status = ?"
        params.append(status)

    sql = f"""
        SELECT s.*,
               (SELECT COUNT(*)
                FROM {db.schema}.ExclusionMatches m
                WHERE m.SessionId = s.Id
                  AND m.RiskLevel IN ('Critical', 'High')
               ) AS TotalHighRisk
        FROM {db.schema}.AnalysisSessions s
        {where}
        ORDER BY s.Id DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """
    params.extend([offset, limit])

    cursor  = db.execute(sql, tuple(params))
    columns = [col[0] for col in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

    # Column name mapping (DB PascalCase → schema snake_case)
    col_map = {
        "Id":               "id",
        "ErectorNameRaw":   "erector_name_raw",
        "JobNumber":        "job_number",
        "JobName":          "job_name",
        "SourceFileName":   "source_file_name",
        "Status":           "status",
        "TotalExtracted":   "total_extracted",
        "TotalClassified":  "total_classified",
        "TotalAligned":     "total_aligned",
        "TotalErectorOnly": "total_erector_only",
        "TotalMfcOnly":     "total_mfc_only",
        "TotalPartial":     "total_partial",
        "TotalHighRisk":    "total_high_risk",
        "CreatedAt":        "created_at",
        "CompletedAt":      "completed_at",
    }

    sessions = []
    for row in rows:
        mapped = {snake: row.get(pascal) for pascal, snake in col_map.items()}
        sessions.append(SessionListItem(**mapped))

    return SessionListResponse(sessions=sessions, count=len(sessions))


@router.get("/{session_id}")
async def get_session(
        session_id: int,
        repo: SessionRepo  = Depends(get_session_repo),
        match_repo: MatchRepo = Depends(get_match_repo),
        user: User = Depends(require_active_user) ) -> dict:
    """

    Full session detail including match summary.
    """

    session = repo.get_by_id(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    summary = match_repo.get_match_summary(session_id)

    return {
        "session": session,
        "match_summary": summary,
    }


@router.patch("/{session_id}")
async def update_session(
        session_id: int,
        body: dict,
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """

    Update editable session metadata (erector name, job number, job name).
    """

    allowed = {
        "erector_name_raw": "ErectorNameRaw",
        "job_number":       "JobNumber",
        "job_name":         "JobName",
    }

    sets:   list[str] = []
    params: list      = []

    for key, col in allowed.items():
        if key in body:
            sets.append(f"{col} = ?")
            params.append(body[key])

    if not sets:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    params.append(session_id)

    sql = f"""
        UPDATE {db.schema}.AnalysisSessions
        SET {', '.join(sets)}
        WHERE Id = ?
    """

    db.execute(sql, tuple(params))
    db.commit()

    return {"updated": session_id}


@router.get("/{session_id}/progress")
async def get_session_progress(
        session_id: int,
        repo:   SessionRepo = Depends(get_session_repo),
        runner: JobRunner   = Depends(get_job_runner),
        user:   User        = Depends(require_active_user) ) -> dict:
    """

    Lightweight progress check for a running analysis.
    Returns current phase, counts, and whether the job is still active.
    """

    session = repo.get_by_id(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    status    = session.get("Status", "Unknown")
    is_active = runner.is_running(session_id)
    error     = runner.get_error(session_id)

    # If thread died but session isn't marked Error, surface it
    if not is_active and error and status not in ("Complete", "Error"):
        repo.update_status(session_id, "Error", error_message=error)
        status = "Error"

    # Clean up finished jobs
    if not is_active:
        runner.cleanup(session_id)

    return {
        "session_id":       session_id,
        "status":           status,
        "is_active":        is_active,
        "erector_name_raw": session.get("ErectorNameRaw"),
        "job_number":       session.get("JobNumber"),
        "source_file_name": session.get("SourceFileName"),
        "total_extracted":  session.get("TotalExtracted"),
        "total_classified": session.get("TotalClassified"),
        "total_aligned":    session.get("TotalAligned"),
        "total_erector_only": session.get("TotalErectorOnly"),
        "total_mfc_only":   session.get("TotalMfcOnly"),
        "total_partial":    session.get("TotalPartial"),
        "error_message":    session.get("ErrorMessage"),
    }


@router.delete("/{session_id}")
async def soft_delete_session(
        session_id: int,
        db   = Depends(get_db),
        user: User = Depends(require_role("admin")) ) -> dict:
    """Soft-delete a session. Admin only."""

    cursor = db.execute(
        f"SELECT Id, IsDeleted FROM {db.schema}.AnalysisSessions WHERE Id = ?",
        (session_id,),
    )
    row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    if row[1]:  # already deleted
        return {"deleted": session_id, "already": True}

    db.execute(
        f"""
        UPDATE {db.schema}.AnalysisSessions
        SET IsDeleted = 1, DeletedAt = SYSUTCDATETIME(), DeletedBy = ?
        WHERE Id = ?
        """,
        (user.display_name, session_id),
    )
    db.commit()

    return {"deleted": session_id}
