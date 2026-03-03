"""Training — verification queue and feedback loop for classification accuracy."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from scope_classification import TrainingRepo

from ..auth import User, require_active_user, require_role
from ..dependencies import get_training_repo, get_db
from ..schemas import (
    TrainingQueueItem,
    TrainingQueueResponse,
    TrainingVerification,
    TrainingStatsResponse,
)


router = APIRouter()
log    = logging.getLogger(__name__)


# -- Queue -----------------------------------------------------------------

@router.get(
    "/queue",
    response_model = TrainingQueueResponse,
)
async def get_training_queue(
        max_confidence: float = Query(0.72, ge=0.0, le=1.0),
        limit:          int   = Query(50, ge=1, le=200),
        offset:         int   = Query(0, ge=0),
        repo: TrainingRepo    = Depends(get_training_repo),
        user: User            = Depends(require_role("estimator", "admin")) ) -> TrainingQueueResponse:
    """Paginated queue of low-confidence items awaiting verification."""

    rows  = repo.get_training_queue(max_confidence=max_confidence, limit=limit, offset=offset)
    total = repo.get_training_queue_count(max_confidence=max_confidence)
    stats = repo.get_verification_stats()

    col_map = {
        "ExtractionId":             "extraction_id",
        "RawText":                  "raw_text",
        "NormalizedText":           "normalized_text",
        "CategoryId":               "category_id",
        "CategoryName":             "category_name",
        "ClassificationConfidence": "classification_confidence",
        "SessionId":                "session_id",
        "ErectorName":              "erector_name",
        "JobNumber":                "job_number",
        "JobName":                  "job_name",
    }

    items = []
    for row in rows:
        mapped = {snake: row.get(pascal) for pascal, snake in col_map.items()}
        items.append(TrainingQueueItem(**mapped))

    return TrainingQueueResponse(
        items            = items,
        total_pending    = total,
        total_verified   = stats.get("TotalVerified", 0) or 0,
        total_overridden = stats.get("TotalOverridden", 0) or 0,
        max_confidence   = max_confidence,
    )


# -- Verify / Correct -----------------------------------------------------

@router.post("/verify")
async def submit_verification(
        body: TrainingVerification,
        repo: TrainingRepo = Depends(get_training_repo),
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Confirm or correct a classification. Feeds into few-shot prompt bank."""

    # Look up the original extraction to get context
    sql = f"""
        SELECT
            ee.RawText,
            ee.CategoryId       AS OriginalCategoryId,
            ee.SessionId,
            s.ErectorId
        FROM {db.schema}.ExtractedExclusions ee
        INNER JOIN {db.schema}.AnalysisSessions s ON ee.SessionId = s.Id
        WHERE ee.Id = ?
    """

    cursor = db.execute(sql, (body.extraction_id,))
    cols   = [col[0] for col in cursor.description]
    row    = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Extraction {body.extraction_id} not found")

    extraction      = dict(zip(cols, row))
    original_cat_id = extraction["OriginalCategoryId"]
    was_overridden  = body.category_id != original_cat_id

    resolved_verifier = body.verified_by or user.display_name

    new_id = repo.insert_verification(
        exclusion_text       = extraction["RawText"],
        category_id          = body.category_id,
        verified_by          = resolved_verifier,
        original_category_id = original_cat_id,
        was_overridden       = was_overridden,
        source_session_id    = extraction["SessionId"],
        source_extraction_id = body.extraction_id,
        erector_id           = extraction.get("ErectorId"),
    )

    action = "corrected" if was_overridden else "confirmed"
    log.info(f"  Training: {action} extraction {body.extraction_id} → category {body.category_id} (id={new_id})")

    return {
        "id":             new_id,
        "was_overridden": was_overridden,
        "action":         action,
    }


# -- Stats -----------------------------------------------------------------

@router.get(
    "/stats",
    response_model = TrainingStatsResponse,
)
async def get_training_stats(
        max_confidence: float = Query(0.72, ge=0.0, le=1.0),
        repo: TrainingRepo    = Depends(get_training_repo),
        user: User            = Depends(require_active_user) ) -> TrainingStatsResponse:
    """Overview stats for the training feedback system."""

    stats   = repo.get_verification_stats()
    pending = repo.get_training_queue_count(max_confidence=max_confidence)

    total_verified  = stats.get("TotalVerified", 0) or 0
    total_overridden = stats.get("TotalOverridden", 0) or 0

    accuracy_rate = None
    if total_verified > 0:
        accuracy_rate = round((total_verified - total_overridden) / total_verified, 4)

    return TrainingStatsResponse(
        total_verified   = total_verified,
        total_overridden = total_overridden,
        total_pending    = pending,
        accuracy_rate    = accuracy_rate,
    )
