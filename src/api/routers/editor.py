"""Editor persistence — save/restore region removals, paragraph removals, and text edits."""

import logging

from fastapi import APIRouter, Depends, HTTPException

from ..auth import User, require_active_user
from ..dependencies import get_db
from ..schemas import (
    EditorRemoveRegionRequest,
    EditorRemoveParagraphRequest,
    EditorTextEditRequest,
    EditorRestoreRegionRequest,
    EditorRestoreParagraphRequest,
)


router = APIRouter()
log    = logging.getLogger(__name__)


# ── Remove / Restore Regions ─────────────────────────────────────────

@router.post("/session/{session_id}/remove-region")
async def remove_region(
        session_id: int,
        body: EditorRemoveRegionRequest,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Persist removal of an MFC exclusion region."""

    sql = f"""
        MERGE {db.schema}.EditorRemovedRegions AS target
        USING (SELECT ? AS SessionId, ? AS MfcExclusionId) AS source
        ON target.SessionId = source.SessionId
           AND target.MfcExclusionId = source.MfcExclusionId
        WHEN NOT MATCHED THEN
            INSERT (SessionId, MfcExclusionId, ParaIndex, RemovedBy)
            VALUES (?, ?, ?, ?);
    """

    db.execute(sql, (
        session_id, body.mfc_exclusion_id,
        session_id, body.mfc_exclusion_id, body.para_index, user.email,
    ))

    log.info(f"Region removed: session={session_id}, mfc={body.mfc_exclusion_id}, by={user.email}")

    return {"status": "removed"}


@router.post("/session/{session_id}/restore-region")
async def restore_region(
        session_id: int,
        body: EditorRestoreRegionRequest,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Restore a previously removed MFC exclusion region."""

    sql = f"""
        DELETE FROM {db.schema}.EditorRemovedRegions
        WHERE SessionId = ? AND MfcExclusionId = ?
    """

    db.execute(sql, (session_id, body.mfc_exclusion_id))

    log.info(f"Region restored: session={session_id}, mfc={body.mfc_exclusion_id}, by={user.email}")

    return {"status": "restored"}


# ── Remove / Restore Paragraphs ──────────────────────────────────────

@router.post("/session/{session_id}/remove-paragraph")
async def remove_paragraph(
        session_id: int,
        body: EditorRemoveParagraphRequest,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Persist removal of a whole paragraph."""

    sql = f"""
        MERGE {db.schema}.EditorRemovedParagraphs AS target
        USING (SELECT ? AS SessionId, ? AS ParaIndex) AS source
        ON target.SessionId = source.SessionId
           AND target.ParaIndex = source.ParaIndex
        WHEN NOT MATCHED THEN
            INSERT (SessionId, ParaIndex, RemovedBy)
            VALUES (?, ?, ?);
    """

    db.execute(sql, (
        session_id, body.para_index,
        session_id, body.para_index, user.email,
    ))

    log.info(f"Paragraph removed: session={session_id}, para={body.para_index}, by={user.email}")

    return {"status": "removed"}


@router.post("/session/{session_id}/restore-paragraph")
async def restore_paragraph(
        session_id: int,
        body: EditorRestoreParagraphRequest,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Restore a previously removed paragraph."""

    sql = f"""
        DELETE FROM {db.schema}.EditorRemovedParagraphs
        WHERE SessionId = ? AND ParaIndex = ?
    """

    db.execute(sql, (session_id, body.para_index))

    log.info(f"Paragraph restored: session={session_id}, para={body.para_index}, by={user.email}")

    return {"status": "restored"}


# ── Text Edits ───────────────────────────────────────────────────────

@router.put("/session/{session_id}/text-edit")
async def save_text_edit(
        session_id: int,
        body: EditorTextEditRequest,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Save or update a text edit for a paragraph (upsert)."""

    sql = f"""
        MERGE {db.schema}.EditorTextEdits AS target
        USING (SELECT ? AS SessionId, ? AS ParaIndex) AS source
        ON target.SessionId = source.SessionId
           AND target.ParaIndex = source.ParaIndex
        WHEN MATCHED THEN
            UPDATE SET EditedText = ?, EditedBy = ?, EditedAt = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (SessionId, ParaIndex, EditedText, EditedBy)
            VALUES (?, ?, ?, ?);
    """

    db.execute(sql, (
        session_id, body.para_index,
        body.edited_text, user.email,
        session_id, body.para_index, body.edited_text, user.email,
    ))

    log.info(f"Text edit saved: session={session_id}, para={body.para_index}, by={user.email}")

    return {"status": "saved"}


# ── Reset All Edits ──────────────────────────────────────────────────

@router.post("/session/{session_id}/reset")
async def reset_all_edits(
        session_id: int,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Reset all editor state for a session — removes all persisted edits."""

    schema = db.schema

    db.execute(f"DELETE FROM {schema}.EditorRemovedRegions WHERE SessionId = ?", (session_id,))
    db.execute(f"DELETE FROM {schema}.EditorRemovedParagraphs WHERE SessionId = ?", (session_id,))
    db.execute(f"DELETE FROM {schema}.EditorTextEdits WHERE SessionId = ?", (session_id,))

    log.info(f"Editor reset: session={session_id}, by={user.email}")

    return {"status": "reset"}
