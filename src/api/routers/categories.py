"""Category endpoints — list, CRUD, and heatmap data."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from scope_classification import ExclusionRepo

from ..auth import User, require_active_user, require_role
from ..dependencies import get_exclusion_repo, get_db


router = APIRouter()


class CategoryCreate(BaseModel):
    name:        str
    description: str = ""
    sort_order:  int = 0


class CategoryUpdate(BaseModel):
    name:        str | None = None
    description: str | None = None
    sort_order:  int | None = None


@router.get("")
async def list_categories(
        repo: ExclusionRepo = Depends(get_exclusion_repo),
        user: User          = Depends(require_active_user) ) -> dict:
    """All exclusion categories from the reference table."""

    categories = repo.get_all_categories()

    return {"categories": categories, "count": len(categories)}


@router.get("/heatmap")
async def category_heatmap(
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Gap distribution matrix: category × match type across all sessions."""

    sql = f"""
        SELECT
            c.Id            AS category_id,
            c.Name          AS category_name,
            m.MatchType,
            COUNT(*)        AS cnt
        FROM {db.schema}.ExclusionMatches m
        JOIN {db.schema}.ScopeCategories c ON c.Id = m.CategoryId
        GROUP BY c.Id, c.Name, m.MatchType
        ORDER BY c.Id
    """

    cursor  = db.execute(sql)
    columns = [col[0] for col in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

    return {"data": rows}


@router.post("")
async def create_category(
        body: CategoryCreate,
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Create a new scope category."""

    sql = f"""
        INSERT INTO {db.schema}.ScopeCategories (Name, Description, SortOrder)
        OUTPUT INSERTED.*
        VALUES (?, ?, ?)
    """

    cursor = db.execute(sql, (body.name, body.description, body.sort_order))
    row    = cursor.fetchone()
    db.commit()

    if not row:
        raise HTTPException(status_code=500, detail="Insert failed")

    cols = [col[0] for col in cursor.description]

    return dict(zip(cols, row))


@router.put("/{category_id}")
async def update_category(
        category_id: int,
        body: CategoryUpdate,
        db   = Depends(get_db),
        user: User = Depends(require_role("estimator", "admin")) ) -> dict:
    """Update an existing scope category."""

    sets   = []
    params = []

    if body.name is not None:
        sets.append("Name = ?")
        params.append(body.name)

    if body.description is not None:
        sets.append("Description = ?")
        params.append(body.description)

    if body.sort_order is not None:
        sets.append("SortOrder = ?")
        params.append(body.sort_order)

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(category_id)
    sql = f"UPDATE {db.schema}.ScopeCategories SET {', '.join(sets)} WHERE Id = ?"
    db.execute(sql, tuple(params))
    db.commit()

    cursor = db.execute(f"SELECT * FROM {db.schema}.ScopeCategories WHERE Id = ?", (category_id,))
    row    = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Category {category_id} not found")

    cols = [col[0] for col in cursor.description]

    return dict(zip(cols, row))


@router.delete("/{category_id}")
async def delete_category(
        category_id: int,
        db   = Depends(get_db),
        user: User = Depends(require_role("admin")) ) -> dict:
    """Delete a scope category. Fails if MFC exclusions still reference it."""

    # Check for references
    cursor = db.execute(
        f"SELECT COUNT(*) FROM {db.schema}.MfcExclusions WHERE CategoryId = ?", (category_id,)
    )
    count = cursor.fetchone()[0]

    if count > 0:
        raise HTTPException(
            status_code = 409,
            detail      = f"Category {category_id} still has {count} MFC exclusion(s). Remove them first.",
        )

    cursor = db.execute(f"SELECT Id FROM {db.schema}.ScopeCategories WHERE Id = ?", (category_id,))

    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail=f"Category {category_id} not found")

    db.execute(f"DELETE FROM {db.schema}.ScopeCategories WHERE Id = ?", (category_id,))
    db.commit()

    return {"deleted": category_id}
