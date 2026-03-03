"""FastAPI dependencies for authentication and authorization."""

import logging
from typing import Callable

from fastapi import Depends, HTTPException, Request, status

from .azure_auth import validate_token
from .user_service import UserService
from .user import User


log = logging.getLogger(__name__)


def _get_user_service(request: Request) -> UserService:
    """Build a UserService from the engine's connection factory."""

    return UserService(request.app.state.engine._db)


async def get_current_user(
        token:   dict        = Depends(validate_token),
        service: UserService = Depends(_get_user_service) ) -> User:
    """
    Validate Azure AD token and return User object.
    Creates user record on first sign-in.
    """

    azure_oid    = token.get("oid")
    email        = token.get("preferred_username") or token.get("email") or token.get("upn", "")
    display_name = token.get("name", "")

    if not azure_oid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token: missing object ID",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = service.get_or_create(azure_oid, email, display_name)
    log.info(f"Authenticated: {user.email} (role={user.role})")

    return user


def require_role(*allowed_roles: str) -> Callable:
    """Dependency factory for role-based access control."""

    async def role_checker(user: User = Depends(get_current_user)) -> User:
        """Check that the authenticated user has a permitted role."""

        if user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required: {allowed_roles}, yours: {user.role}",
            )

        return user

    return role_checker


async def require_active_user(user: User = Depends(get_current_user)) -> User:
    """Require user to be activated (not pending)."""

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account pending activation. Contact an administrator.",
        )

    return user
