"""MFC Exclusions — list, detail, create, update, delete."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..dependencies import get_db, get_exclusion_repo
from scope_classification import ExclusionRepo


router = APIRouter()


class MfcExclusionCreate(BaseModel):
    category_id: int
    exclusion:   str
    item_type:   str = "Exclusion"
    sort_order:  int = 0


class MfcExclusionUpdate(BaseModel):
    category_id: int | None = None
    exclusion:   str | None = None
    item_type:   str | None = None
    sort_order:  int | None = None


@router.get("")
async def list_mfc_exclusions(
        category_id: int | None = None,
        repo: ExclusionRepo = Depends(get_exclusion_repo) ) -> dict:
    """All MFC exclusions, optionally filtered by category."""

    exclusions = repo.get_mfc_exclusions(category_id)

    return {"exclusions": exclusions, "count": len(exclusions)}


@router.get("/{exclusion_id}")
async def get_mfc_exclusion(exclusion_id: int, db = Depends(get_db)) -> dict:
    """Single MFC exclusion by Id."""

    sql    = f"SELECT * FROM {db.schema}.MfcExclusions WHERE Id = ?"
    cursor = db.execute(sql, (exclusion_id,))
    row    = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"MFC exclusion {exclusion_id} not found")

    cols = [col[0] for col in cursor.description]

    return dict(zip(cols, row))


@router.post("")
async def create_mfc_exclusion(body: MfcExclusionCreate, db = Depends(get_db)) -> dict:
    """Create a new MFC exclusion."""

    sql = f"""
        INSERT INTO {db.schema}.MfcExclusions (CategoryId, Exclusion, ItemType, SortOrder)
        OUTPUT INSERTED.*
        VALUES (?, ?, ?, ?)
    """

    cursor = db.execute(sql, (body.category_id, body.exclusion, body.item_type, body.sort_order))
    row    = cursor.fetchone()
    db.commit()

    if not row:
        raise HTTPException(status_code=500, detail="Insert failed")

    cols = [col[0] for col in cursor.description]

    return dict(zip(cols, row))


@router.put("/{exclusion_id}")
async def update_mfc_exclusion(exclusion_id: int, body: MfcExclusionUpdate, db = Depends(get_db)) -> dict:
    """Update an existing MFC exclusion."""

    sets   = []
    params = []

    if body.category_id is not None:
        sets.append("CategoryId = ?")
        params.append(body.category_id)

    if body.exclusion is not None:
        sets.append("Exclusion = ?")
        params.append(body.exclusion)

    if body.item_type is not None:
        sets.append("ItemType = ?")
        params.append(body.item_type)

    if body.sort_order is not None:
        sets.append("SortOrder = ?")
        params.append(body.sort_order)

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(exclusion_id)
    sql = f"UPDATE {db.schema}.MfcExclusions SET {', '.join(sets)} WHERE Id = ?"
    db.execute(sql, tuple(params))
    db.commit()

    # Re-fetch updated row
    cursor = db.execute(f"SELECT * FROM {db.schema}.MfcExclusions WHERE Id = ?", (exclusion_id,))
    row    = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"MFC exclusion {exclusion_id} not found")

    cols = [col[0] for col in cursor.description]

    return dict(zip(cols, row))


@router.delete("/{exclusion_id}")
async def delete_mfc_exclusion(exclusion_id: int, db = Depends(get_db)) -> dict:
    """Delete an MFC exclusion."""

    cursor = db.execute(
        f"SELECT Id FROM {db.schema}.MfcExclusions WHERE Id = ?", (exclusion_id,)
    )

    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail=f"MFC exclusion {exclusion_id} not found")

    db.execute(f"DELETE FROM {db.schema}.MfcExclusions WHERE Id = ?", (exclusion_id,))
    db.commit()

    return {"deleted": exclusion_id}
