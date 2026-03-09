"""Admin endpoints — user management for administrators."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..auth import User, require_role
from ..auth.user_service import UserService
from ..dependencies import get_db


router = APIRouter()
log    = logging.getLogger(__name__)


VALID_ROLES = {"pending", "viewer", "estimator", "admin"}


class RoleUpdate(BaseModel):
    role: str


def _get_user_service(request) -> UserService:
    from fastapi import Request
    return UserService(request.app.state.engine._db)


@router.get("/users")
async def list_users(
        db   = Depends(get_db),
        user: User = Depends(require_role("admin")) ) -> dict:
    """List all registered users. Admin only."""

    sql = f"""
        SELECT Id, AzureObjectId, Email, DisplayName, Role, CreatedAt, LastLoginAt
        FROM {db.schema}.Users
        ORDER BY CreatedAt DESC
    """

    cursor  = db.execute(sql)
    columns = [col[0] for col in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

    col_map = {
        "Id":              "id",
        "AzureObjectId":   "azure_object_id",
        "Email":           "email",
        "DisplayName":     "display_name",
        "Role":            "role",
        "CreatedAt":       "created_at",
        "LastLoginAt":     "last_login_at",
    }

    users = []
    for row in rows:
        mapped = {}
        for pascal, snake in col_map.items():
            val = row.get(pascal)
            if snake in ("created_at", "last_login_at") and val is not None:
                val = str(val)
            mapped[snake] = val
        users.append(mapped)

    return {"users": users, "count": len(users)}


@router.patch("/users/{user_id}/role")
async def set_user_role(
        user_id: int,
        body:    RoleUpdate,
        db   = Depends(get_db),
        user: User = Depends(require_role("admin")) ) -> dict:
    """Update a user's role. Admin only."""

    if body.role not in VALID_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role '{body.role}'. Must be one of: {', '.join(sorted(VALID_ROLES))}",
        )

    if user_id == user.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot change your own role.",
        )

    # Verify user exists
    cursor = db.execute(
        f"SELECT Id, Email, Role FROM {db.schema}.Users WHERE Id = ?",
        (user_id,),
    )
    row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")

    old_role = row[2]

    db.execute(
        f"UPDATE {db.schema}.Users SET Role = ? WHERE Id = ?",
        (body.role, user_id),
    )
    db.commit()

    log.info(f"Admin {user.email} changed user {row[1]} role: {old_role} → {body.role}")

    return {"user_id": user_id, "old_role": old_role, "new_role": body.role}
