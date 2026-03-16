"""User info — authenticated user profile and preferences."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from ..auth import User, get_current_user
from ..auth.user_service import UserService


router = APIRouter()
log    = logging.getLogger(__name__)


def _get_user_service(request: Request) -> UserService:
    return UserService(request.app.state.engine._db)


@router.get("/me")
async def get_current_user_info(user: User = Depends(get_current_user)) -> dict:
    """Return the authenticated user's profile."""

    return {
        "id":                  user.id,
        "email":               user.email,
        "display_name":        user.display_name,
        "role":                user.role,
        "is_active":           user.is_active,
        "is_estimator":        user.is_estimator,
        "is_admin":            user.is_admin,
        "highlight_intensity": user.highlight_intensity,
    }


class UpdatePreferencesRequest(BaseModel):
    """Updateable user preferences."""

    highlight_intensity: str | None = None


@router.patch("/me/preferences")
async def update_preferences(
        body:    UpdatePreferencesRequest,
        user:    User        = Depends(get_current_user),
        service: UserService = Depends(_get_user_service) ) -> dict:
    """Update the authenticated user's preferences."""

    if body.highlight_intensity is not None:
        try:
            service.set_highlight_intensity(user.id, body.highlight_intensity)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    return {"status": "updated"}
