"""Erector ↔ MFC exclusion mapping — browse, link, bulk-link, disposition."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..auth import User, require_active_user, require_role
from ..dependencies import get_db

log    = logging.getLogger(__name__)
router = APIRouter()

VALID_DISPOSITIONS = {"Unmapped", "Mapped", "PMReportOnly"}


# ── Request Models ───────────────────────────────────────────────────

class DispositionUpdate(BaseModel):
    disposition: str


class CreateLink(BaseModel):
    erector_exclusion_id: int
    mfc_exclusion_id:     int


class BulkLink(BaseModel):
    erector_exclusion_ids: list[int]
    mfc_exclusion_id:      int


# ── Helpers ──────────────────────────────────────────────────────────

def _row_to_dict(cursor, row) -> dict:
    """Convert a pyodbc row to a dict using cursor.description."""

    cols = [col[0] for col in cursor.description]

    return dict(zip(cols, row))


def _set_disposition(
        db,
        erector_exclusion_id: int,
        disposition: str,
        user_name: str ) -> None:
    """Update disposition + audit fields on an ErectorExclusion."""

    now = datetime.now(timezone.utc)

    db.execute(
        f"""
        UPDATE {db.schema}.ErectorExclusions
        SET Disposition = ?, MappedBy = ?, MappedAt = ?
        WHERE Id = ?
        """,
        (disposition, user_name, now, erector_exclusion_id),
    )


# ── GET /erector-exclusions ──────────────────────────────────────────

@router.get("/erector-exclusions")
async def list_erector_exclusions(
        category_id: int | None = None,
        erector_id:  int | None = None,
        disposition: str | None = None,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """All erector exclusions with nested MFC mapping links."""

    # ── Build the erector exclusions query ────────────────────────
    where   = []
    params  = []

    if category_id is not None:
        where.append("ee.CategoryId = ?")
        params.append(category_id)

    if erector_id is not None:
        where.append("ee.ErectorId = ?")
        params.append(erector_id)

    if disposition is not None:
        where.append("ee.Disposition = ?")
        params.append(disposition)

    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
        SELECT
            ee.Id,
            ee.ErectorId,
            e.Name         AS ErectorName,
            e.ShortName    AS ErectorShortName,
            ee.CategoryId,
            c.Name         AS CategoryName,
            ee.Exclusion,
            ee.IsStandard,
            ee.ItemType,
            ee.Disposition,
            ee.MappedBy,
            ee.MappedAt,
            ee.Notes
        FROM {db.schema}.ErectorExclusions ee
        JOIN {db.schema}.Erectors e         ON e.Id = ee.ErectorId
        JOIN {db.schema}.ScopeCategories c  ON c.Id = ee.CategoryId
        {where_clause}
        ORDER BY c.Name, e.ShortName, ee.SortOrder
    """

    cursor = db.execute(sql, tuple(params))
    cols   = [col[0] for col in cursor.description]
    rows   = [dict(zip(cols, r)) for r in cursor.fetchall()]

    if not rows:
        return {"items": [], "count": 0}

    # ── Fetch all mapping links for these items in one query ─────
    ee_ids = [r["Id"] for r in rows]

    placeholders = ",".join("?" * len(ee_ids))

    link_sql = f"""
        SELECT
            m.Id            AS LinkId,
            m.ErectorExclusionId,
            m.MfcExclusionId,
            mfc.Exclusion   AS MfcExclusion,
            mfc.CategoryId  AS MfcCategoryId,
            mc.Name         AS MfcCategoryName,
            m.CreatedBy,
            m.CreatedAt
        FROM {db.schema}.ErectorMfcMappings m
        JOIN {db.schema}.MfcExclusions mfc  ON mfc.Id = m.MfcExclusionId
        JOIN {db.schema}.ScopeCategories mc ON mc.Id  = mfc.CategoryId
        WHERE m.ErectorExclusionId IN ({placeholders})
        ORDER BY m.ErectorExclusionId, mfc.CategoryId, mfc.SortOrder
    """

    link_cursor = db.execute(link_sql, tuple(ee_ids))
    link_cols   = [col[0] for col in link_cursor.description]
    link_rows   = [dict(zip(link_cols, r)) for r in link_cursor.fetchall()]

    # ── Group links by erector exclusion id ──────────────────────
    links_by_ee = {}
    for link in link_rows:
        eid = link["ErectorExclusionId"]
        links_by_ee.setdefault(eid, []).append({
            "link_id":           link["LinkId"],
            "mfc_exclusion_id":  link["MfcExclusionId"],
            "mfc_exclusion":     link["MfcExclusion"],
            "mfc_category_id":   link["MfcCategoryId"],
            "mfc_category_name": link["MfcCategoryName"],
            "created_by":        link["CreatedBy"],
            "created_at":        str(link["CreatedAt"]),
        })

    # ── Attach mappings to each row ──────────────────────────────
    for row in rows:
        row["mappings"] = links_by_ee.get(row["Id"], [])

    return {"items": rows, "count": len(rows)}


# ── GET /mfc-options ─────────────────────────────────────────────────

@router.get("/mfc-options")
async def list_mfc_options(
        category_id: int | None = None,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Lightweight MFC exclusion list for dropdown population."""

    where  = ""
    params = ()

    if category_id is not None:
        where  = "WHERE mfc.CategoryId = ?"
        params = (category_id,)

    sql = f"""
        SELECT
            mfc.Id,
            mfc.Exclusion,
            mfc.CategoryId,
            c.Name AS CategoryName,
            mfc.ScopeType
        FROM {db.schema}.MfcExclusions mfc
        JOIN {db.schema}.ScopeCategories c ON c.Id = mfc.CategoryId
        {where}
        ORDER BY c.Name, mfc.SortOrder
    """

    cursor = db.execute(sql, params)
    cols   = [col[0] for col in cursor.description]
    items  = [dict(zip(cols, r)) for r in cursor.fetchall()]

    return {"items": items, "count": len(items)}


# ── PATCH /erector-exclusions/{id}/disposition ───────────────────────

@router.patch("/erector-exclusions/{erector_exclusion_id}/disposition")
async def update_disposition(
        erector_exclusion_id: int,
        body: DispositionUpdate,
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Set the disposition on an erector exclusion."""

    if body.disposition not in VALID_DISPOSITIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid disposition '{body.disposition}'. Must be one of: {', '.join(sorted(VALID_DISPOSITIONS))}",
        )

    # Verify the erector exclusion exists
    cursor = db.execute(
        f"SELECT Id FROM {db.schema}.ErectorExclusions WHERE Id = ?",
        (erector_exclusion_id,),
    )

    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail=f"Erector exclusion {erector_exclusion_id} not found")

    _set_disposition(db, erector_exclusion_id, body.disposition, user.display_name)
    db.commit()

    log.info(f"Disposition updated: ErectorExclusion {erector_exclusion_id} → {body.disposition} by {user.display_name}")

    return {"erector_exclusion_id": erector_exclusion_id, "disposition": body.disposition}


# ── POST /links ──────────────────────────────────────────────────────

@router.post("/links")
async def create_link(
        body: CreateLink,
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Create a single erector → MFC mapping link."""

    ee_id  = body.erector_exclusion_id
    mfc_id = body.mfc_exclusion_id

    # Validate erector exclusion exists
    cursor = db.execute(
        f"SELECT Id, Disposition FROM {db.schema}.ErectorExclusions WHERE Id = ?",
        (ee_id,),
    )
    ee_row = cursor.fetchone()

    if not ee_row:
        raise HTTPException(status_code=404, detail=f"Erector exclusion {ee_id} not found")

    # Validate MFC exclusion exists
    cursor = db.execute(
        f"SELECT Id FROM {db.schema}.MfcExclusions WHERE Id = ?",
        (mfc_id,),
    )

    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail=f"MFC exclusion {mfc_id} not found")

    # Check for duplicate
    cursor = db.execute(
        f"""
        SELECT Id FROM {db.schema}.ErectorMfcMappings
        WHERE ErectorExclusionId = ? AND MfcExclusionId = ?
        """,
        (ee_id, mfc_id),
    )

    if cursor.fetchone():
        raise HTTPException(status_code=409, detail="This mapping link already exists")

    # Insert the link
    now = datetime.now(timezone.utc)

    cursor = db.execute(
        f"""
        INSERT INTO {db.schema}.ErectorMfcMappings
            (ErectorExclusionId, MfcExclusionId, CreatedBy, CreatedAt)
        OUTPUT INSERTED.*
        VALUES (?, ?, ?, ?)
        """,
        (ee_id, mfc_id, user.display_name, now),
    )
    link_row = _row_to_dict(cursor, cursor.fetchone())

    # Auto-update disposition to Mapped if currently Unmapped
    current_disposition = ee_row[1]

    if current_disposition == "Unmapped":
        _set_disposition(db, ee_id, "Mapped", user.display_name)

    db.commit()

    log.info(f"Mapping created: ErectorExclusion {ee_id} → MfcExclusion {mfc_id} by {user.display_name}")

    return link_row


# ── DELETE /links/{link_id} ──────────────────────────────────────────

@router.delete("/links/{link_id}")
async def delete_link(
        link_id: int,
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Remove a mapping link. Auto-resets disposition if no links remain."""

    # Fetch the link to get the erector exclusion id
    cursor = db.execute(
        f"SELECT ErectorExclusionId FROM {db.schema}.ErectorMfcMappings WHERE Id = ?",
        (link_id,),
    )
    link_row = cursor.fetchone()

    if not link_row:
        raise HTTPException(status_code=404, detail=f"Mapping link {link_id} not found")

    ee_id = link_row[0]

    # Delete the link
    db.execute(
        f"DELETE FROM {db.schema}.ErectorMfcMappings WHERE Id = ?",
        (link_id,),
    )

    # Check remaining link count for this erector exclusion
    cursor = db.execute(
        f"SELECT COUNT(*) FROM {db.schema}.ErectorMfcMappings WHERE ErectorExclusionId = ?",
        (ee_id,),
    )
    remaining = cursor.fetchone()[0]

    # Auto-reset to Unmapped if that was the last link and disposition is Mapped
    if remaining == 0:
        cursor = db.execute(
            f"SELECT Disposition FROM {db.schema}.ErectorExclusions WHERE Id = ?",
            (ee_id,),
        )
        current = cursor.fetchone()

        if current and current[0] == "Mapped":
            _set_disposition(db, ee_id, "Unmapped", user.display_name)

    db.commit()

    log.info(f"Mapping link {link_id} deleted by {user.display_name}")

    return {"deleted": link_id, "remaining_links": remaining}


# ── POST /bulk-link ──────────────────────────────────────────────────

@router.post("/bulk-link")
async def bulk_link(
        body: BulkLink,
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Map multiple erector exclusions to one MFC exclusion in one shot."""

    mfc_id = body.mfc_exclusion_id
    ee_ids = body.erector_exclusion_ids

    if not ee_ids:
        raise HTTPException(status_code=400, detail="erector_exclusion_ids must not be empty")

    # Validate MFC exclusion exists
    cursor = db.execute(
        f"SELECT Id FROM {db.schema}.MfcExclusions WHERE Id = ?",
        (mfc_id,),
    )

    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail=f"MFC exclusion {mfc_id} not found")

    # Validate all erector exclusion ids exist
    placeholders = ",".join("?" * len(ee_ids))

    cursor = db.execute(
        f"SELECT Id FROM {db.schema}.ErectorExclusions WHERE Id IN ({placeholders})",
        tuple(ee_ids),
    )
    found_ids = {row[0] for row in cursor.fetchall()}
    missing   = set(ee_ids) - found_ids

    if missing:
        raise HTTPException(status_code=404, detail=f"Erector exclusions not found: {sorted(missing)}")

    # Fetch existing links to avoid duplicates
    cursor = db.execute(
        f"""
        SELECT ErectorExclusionId
        FROM {db.schema}.ErectorMfcMappings
        WHERE ErectorExclusionId IN ({placeholders}) AND MfcExclusionId = ?
        """,
        tuple(ee_ids) + (mfc_id,),
    )
    already_linked = {row[0] for row in cursor.fetchall()}
    to_insert      = [eid for eid in ee_ids if eid not in already_linked]

    # Insert new links
    now     = datetime.now(timezone.utc)
    created = 0

    for eid in to_insert:
        db.execute(
            f"""
            INSERT INTO {db.schema}.ErectorMfcMappings
                (ErectorExclusionId, MfcExclusionId, CreatedBy, CreatedAt)
            VALUES (?, ?, ?, ?)
            """,
            (eid, mfc_id, user.display_name, now),
        )
        created += 1

    # Update dispositions for all items in the batch
    for eid in ee_ids:
        _set_disposition(db, eid, "Mapped", user.display_name)

    db.commit()

    log.info(
        f"Bulk link: {created} new links to MfcExclusion {mfc_id}, "
        f"{len(already_linked)} skipped (duplicates) — by {user.display_name}"
    )

    return {
        "mfc_exclusion_id": mfc_id,
        "links_created":    created,
        "links_skipped":    len(already_linked),
        "total_mapped":     len(ee_ids),
    }


# ── GET /stats ───────────────────────────────────────────────────────

@router.get("/stats")
async def mapping_stats(
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Mapping progress summary — overall and per-erector."""

    # Overall counts by disposition
    cursor = db.execute(f"""
        SELECT Disposition, COUNT(*) AS Cnt
        FROM {db.schema}.ErectorExclusions
        GROUP BY Disposition
    """)
    by_disposition = {row[0]: row[1] for row in cursor.fetchall()}

    total = sum(by_disposition.values())

    # Per-erector breakdown
    cursor = db.execute(f"""
        SELECT
            e.ShortName,
            ee.Disposition,
            COUNT(*) AS Cnt
        FROM {db.schema}.ErectorExclusions ee
        JOIN {db.schema}.Erectors e ON e.Id = ee.ErectorId
        GROUP BY e.ShortName, ee.Disposition
        ORDER BY e.ShortName, ee.Disposition
    """)

    by_erector = {}
    for row in cursor.fetchall():
        name        = row[0]
        disposition = row[1]
        count       = row[2]

        if name not in by_erector:
            by_erector[name] = {"total": 0}

        by_erector[name][disposition] = count
        by_erector[name]["total"]    += count

    return {
        "total":          total,
        "by_disposition": by_disposition,
        "by_erector":     by_erector,
    }
