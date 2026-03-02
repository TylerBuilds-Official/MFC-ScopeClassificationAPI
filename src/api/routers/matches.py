"""Match endpoints — per-session and cross-session high-risk views."""

from fastapi import APIRouter, Depends, HTTPException

from scope_classification import MatchRepo

from ..dependencies import get_match_repo, get_db
from ..schemas import MatchRow, MatchListResponse


router = APIRouter()


@router.get(
    "/session/{session_id}",
    response_model = MatchListResponse,
)
async def get_session_matches(
        session_id: int,
        risk: str | None        = None,
        category_id: int | None = None,
        db                      = Depends(get_db) ) -> MatchListResponse:
    """

    All matches for a session with erector + MFC exclusion text joined in.
    """

    where  = "WHERE m.SessionId = ?"
    params: list = [session_id]

    if category_id is not None:
        where += " AND m.CategoryId = ?"
        params.append(category_id)

    if risk:
        where += " AND m.RiskLevel = ?"
        params.append(risk)

    sql = f"""
        SELECT
            m.Id,
            m.SessionId,
            m.ExtractedExclusionId,
            m.MfcExclusionId,
            m.CategoryId,
            m.MatchType,
            m.Confidence,
            m.AiReasoning,
            m.RiskLevel,
            m.RiskNotes,
            ee.RawText          AS ErectorExclusionText,
            mfc.Exclusion       AS MfcExclusionText,
            mfc.ItemType        AS MfcItemType
        FROM {db.schema}.ExclusionMatches m
        LEFT JOIN {db.schema}.ExtractedExclusions ee
            ON ee.Id = m.ExtractedExclusionId
        LEFT JOIN {db.schema}.MfcExclusions mfc
            ON mfc.Id = m.MfcExclusionId
        {where}
        ORDER BY m.CategoryId, m.Id
    """

    cursor  = db.execute(sql, tuple(params))
    columns = [col[0] for col in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

    col_map = {
        "Id":                     "id",
        "SessionId":              "session_id",
        "ExtractedExclusionId":   "extracted_exclusion_id",
        "MfcExclusionId":         "mfc_exclusion_id",
        "CategoryId":             "category_id",
        "MatchType":              "match_type",
        "Confidence":             "confidence",
        "AiReasoning":            "ai_reasoning",
        "RiskLevel":              "risk_level",
        "RiskNotes":              "risk_notes",
        "ErectorExclusionText":   "erector_text",
        "MfcExclusionText":       "mfc_text",
        "MfcItemType":            "mfc_item_type",
    }

    matches = []
    for row in rows:
        mapped = {snake: row.get(pascal) for pascal, snake in col_map.items()}
        matches.append(MatchRow(**mapped))

    return MatchListResponse(
        session_id = session_id,
        matches    = matches,
        count      = len(matches),
    )


@router.get("/high-risk")
async def get_high_risk(
        limit: int = 100,
        db = Depends(get_db) ) -> dict:
    """

    Cross-session high-risk matches with exclusion text, ordered by most recent.
    """

    sql = f"""
        SELECT TOP (?)
            m.*,
            s.ErectorNameRaw,
            s.JobNumber,
            s.SourceFileName,
            ee.RawText      AS ErectorExclusionText,
            mfc.Exclusion   AS MfcExclusionText,
            mfc.ItemType    AS MfcItemType
        FROM {db.schema}.ExclusionMatches m
        JOIN {db.schema}.AnalysisSessions s ON s.Id = m.SessionId
        LEFT JOIN {db.schema}.ExtractedExclusions ee
            ON ee.Id = m.ExtractedExclusionId
        LEFT JOIN {db.schema}.MfcExclusions mfc
            ON mfc.Id = m.MfcExclusionId
        WHERE m.RiskLevel IN ('High', 'Critical')
        ORDER BY m.Id DESC
    """

    cursor  = db.execute(sql, (limit,))
    columns = [col[0] for col in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

    return {"matches": rows, "count": len(rows)}
