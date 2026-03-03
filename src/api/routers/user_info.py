"""User info — authenticated user profile endpoint."""

from fastapi import APIRouter, Depends

from ..auth import User, get_current_user


router = APIRouter()


@router.get("/me")
async def get_current_user_info(user: User = Depends(get_current_user)) -> dict:
    """Return the authenticated user's profile."""

    return {
        "id":           user.id,
        "email":        user.email,
        "display_name": user.display_name,
        "role":         user.role,
        "is_active":    user.is_active,
        "is_estimator": user.is_estimator,
        "is_admin":     user.is_admin,
    }
