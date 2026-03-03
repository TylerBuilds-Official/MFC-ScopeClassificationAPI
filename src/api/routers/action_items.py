"""Action Items — estimator triage list derived from session matches."""

import logging

from fastapi import APIRouter, Depends, HTTPException

from ..dependencies import get_db
from ..schemas import (
    ActionItemRow,
    ActionItemSummary,
    ActionItemListResponse,
    ActionItemUpdate,
    ActionItemBatchUpdate,
)


router = APIRouter()
log    = logging.getLogger(__name__)


VALID_STATUSES = {"unreviewed", "acknowledged", "addressed", "dismissed"}


# -- List ------------------------------------------------------------------

@router.get(
    "/session/{session_id}",
    response_model = ActionItemListResponse,
)
async def get_session_action_items(
        session_id: int,
        db = Depends(get_db) ) -> ActionItemListResponse:
    """All action items for a session with match data joined in."""

    sql = f"""
        SELECT
            a.Id,
            a.SessionId,
            a.MatchId,
            a.Section,
            a.Status,
            a.Notes,
            a.CreatedAt,
            a.UpdatedAt,
            m.MatchType,
            m.Confidence,
            m.RiskLevel,
            m.RiskNotes,
            m.AiReasoning,
            m.CategoryId,
            m.MfcExclusionId,
            ee.RawText      AS ErectorExclusionText,
            mfc.Exclusion   AS MfcExclusionText,
            mfc.ItemType    AS MfcItemType
        FROM {db.schema}.ActionItems a
        LEFT JOIN {db.schema}.ExclusionMatches m
            ON m.Id = a.MatchId
        LEFT JOIN {db.schema}.ExtractedExclusions ee
            ON ee.Id = m.ExtractedExclusionId
        LEFT JOIN {db.schema}.MfcExclusions mfc
            ON mfc.Id = m.MfcExclusionId
        WHERE a.SessionId = ?
        ORDER BY
            CASE a.Section
                WHEN 'high_risk'       THEN 0
                WHEN 'erector_only'    THEN 1
                WHEN 'partial_review'  THEN 2
            END,
            CASE m.RiskLevel
                WHEN 'Critical' THEN 0
                WHEN 'High'     THEN 1
                WHEN 'Medium'   THEN 2
                WHEN 'Low'      THEN 3
            END,
            m.Confidence ASC
    """

    cursor  = db.execute(sql, (session_id,))
    columns = [col[0] for col in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

    col_map = {
        "Id":                     "id",
        "SessionId":              "session_id",
        "MatchId":                "match_id",
        "Section":                "section",
        "Status":                 "status",
        "Notes":                  "notes",
        "CreatedAt":              "created_at",
        "UpdatedAt":              "updated_at",
        "MatchType":              "match_type",
        "Confidence":             "confidence",
        "RiskLevel":              "risk_level",
        "RiskNotes":              "risk_notes",
        "AiReasoning":            "ai_reasoning",
        "CategoryId":             "category_id",
        "MfcExclusionId":         "mfc_exclusion_id",
        "ErectorExclusionText":   "erector_text",
        "MfcExclusionText":       "mfc_text",
        "MfcItemType":            "mfc_item_type",
    }

    items = []
    for row in rows:
        mapped = {}
        for pascal, snake in col_map.items():
            val = row.get(pascal)
            if snake in ("created_at", "updated_at") and val is not None:
                val = str(val)
            mapped[snake] = val
        items.append(ActionItemRow(**mapped))

    summary = _build_summary(items)

    return ActionItemListResponse(
        session_id = session_id,
        items      = items,
        summary    = summary,
    )


# -- Batch update (registered before /{item_id} to avoid route conflict) --

@router.patch("/batch")
async def batch_update_action_items(
        body: ActionItemBatchUpdate,
        db = Depends(get_db) ) -> dict:
    """Bulk status update for multiple action items."""

    if body.status not in VALID_STATUSES:
        raise HTTPException(status_code=400, detail=f"Invalid status: {body.status}")

    if not body.item_ids:
        raise HTTPException(status_code=400, detail="No item IDs provided")

    placeholders = ", ".join("?" for _ in body.item_ids)

    sql = f"""
        UPDATE {db.schema}.ActionItems
        SET Status = ?, UpdatedAt = SYSUTCDATETIME()
        WHERE Id IN ({placeholders})
    """

    params = [body.status] + body.item_ids
    db.execute(sql, tuple(params))
    db.commit()

    return {"updated": len(body.item_ids), "status": body.status}


# -- Update single --------------------------------------------------------

@router.patch("/{item_id}")
async def update_action_item(
        item_id: int,
        body:    ActionItemUpdate,
        db = Depends(get_db) ) -> dict:
    """Update status and/or notes on a single action item."""

    sets:   list[str] = []
    params: list      = []

    if body.status is not None:
        if body.status not in VALID_STATUSES:
            raise HTTPException(status_code=400, detail=f"Invalid status: {body.status}")
        sets.append("Status = ?")
        params.append(body.status)

    if body.notes is not None:
        sets.append("Notes = ?")
        params.append(body.notes)

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    sets.append("UpdatedAt = SYSUTCDATETIME()")
    params.append(item_id)

    sql = f"UPDATE {db.schema}.ActionItems SET {', '.join(sets)} WHERE Id = ?"
    db.execute(sql, tuple(params))
    db.commit()

    return {"updated": item_id}


# -- Generate / regenerate ------------------------------------------------

@router.post("/session/{session_id}/generate")
async def generate_action_items(
        session_id: int,
        db = Depends(get_db) ) -> dict:
    """Generate (or regenerate) action items from session matches."""

    count = generate_action_items_for_session(db, session_id)

    return {"session_id": session_id, "generated": count}


# -- Generation logic (reusable) ------------------------------------------

def generate_action_items_for_session(db, session_id: int) -> int:
    """Derive action items from matches. Clears existing items first."""

    # Clear any existing action items for this session
    db.execute(
        f"DELETE FROM {db.schema}.ActionItems WHERE SessionId = ?",
        (session_id,),
    )

    # Fetch all matches for the session
    cursor = db.execute(
        f"""
        SELECT Id, MatchType, Confidence, RiskLevel
        FROM {db.schema}.ExclusionMatches
        WHERE SessionId = ?
        """,
        (session_id,),
    )
    columns = [col[0] for col in cursor.description]
    matches = [dict(zip(columns, r)) for r in cursor.fetchall()]

    items: list[tuple[int, str]] = []

    for m in matches:
        match_id   = m["Id"]
        match_type = m["MatchType"]
        risk_level = m["RiskLevel"]
        confidence = m["Confidence"]

        # Priority: high_risk > erector_only > partial_review
        if risk_level in ("Critical", "High"):
            items.append((match_id, "high_risk"))
        elif match_type == "ErectorOnly":
            items.append((match_id, "erector_only"))
        elif match_type == "Partial" and confidence is not None and confidence < 0.70:
            items.append((match_id, "partial_review"))

    if not items:
        db.commit()

        return 0

    # Bulk insert
    values_clause = ", ".join("(?, ?, ?)" for _ in items)
    params: list  = []
    for match_id, section in items:
        params.extend([session_id, match_id, section])

    sql = f"""
        INSERT INTO {db.schema}.ActionItems (SessionId, MatchId, Section)
        VALUES {values_clause}
    """

    db.execute(sql, tuple(params))
    db.commit()

    log.info(f"  Session {session_id}: generated {len(items)} action items")

    return len(items)


# -- Helpers ---------------------------------------------------------------

def _build_summary(items: list[ActionItemRow]) -> ActionItemSummary:
    """Build summary counts from a list of action items."""

    status_counts  = {"unreviewed": 0, "acknowledged": 0, "addressed": 0, "dismissed": 0}
    section_counts: dict[str, int] = {}

    for item in items:
        status_counts[item.status] = status_counts.get(item.status, 0) + 1
        section_counts[item.section] = section_counts.get(item.section, 0) + 1

    return ActionItemSummary(
        total        = len(items),
        unreviewed   = status_counts["unreviewed"],
        acknowledged = status_counts["acknowledged"],
        addressed    = status_counts["addressed"],
        dismissed    = status_counts["dismissed"],
        by_section   = section_counts,
    )
