from dataclasses import dataclass
from datetime import datetime


@dataclass
class User:
    """Authenticated user from Scope.Users."""

    id:                  int
    azure_object_id:     str
    email:               str
    display_name:        str
    role:                str
    created_at:          datetime
    last_login_at:       datetime | None = None
    highlight_intensity: str             = 'standard'

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def is_estimator(self) -> bool:
        return self.role in ("estimator", "admin")

    @property
    def is_active(self) -> bool:
        """Users with 'pending' role are not yet activated."""

        return self.role in ("viewer", "estimator", "admin")
