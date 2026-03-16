"""User CRUD against Scope.Users via ConnectionFactory."""

import logging
from datetime import datetime

from scope_classification import ConnectionFactory

from .user import User


log = logging.getLogger(__name__)

VALID_ROLES = {"pending", "viewer", "estimator", "admin"}


class UserService:
    """Handles user lookup and creation against Scope.Users."""

    def __init__(self, db: ConnectionFactory) -> None:
        """Initialize with database connection factory."""

        self._db     = db
        self._schema = db.schema


    def get_or_create(self, azure_oid: str, email: str, display_name: str) -> User:
        """
        Find user by Azure OID, fall back to email match, or create new.

        Handles seeded users whose AzureObjectId is a placeholder
        by updating the OID on first real login.
        """

        # Try by Azure OID first
        user = self._find_by_oid(azure_oid)
        if user:
            self._update_last_login(user.id)

            return user

        # Fall back to email match (catches seeded users)
        user = self._find_by_email(email)
        if user:
            self._update_oid_and_login(user.id, azure_oid, display_name)
            user.azure_object_id = azure_oid
            user.last_login_at   = datetime.utcnow()

            return user

        # New user — create with pending role
        return self._create(azure_oid, email, display_name)


    def get_by_id(self, user_id: int) -> User | None:
        """Fetch a user by internal ID."""

        sql = f"""
            SELECT Id, AzureObjectId, Email, DisplayName, Role, CreatedAt, LastLoginAt, HighlightIntensity
            FROM {self._schema}.Users
            WHERE Id = ?
        """

        cursor = self._db.execute(sql, (user_id,))
        row    = cursor.fetchone()

        if not row:

            return None

        return self._row_to_user(row)


    def set_role(self, user_id: int, role: str) -> bool:
        """Set a user's role. Returns True if updated."""

        if role not in VALID_ROLES:
            raise ValueError(f"Invalid role '{role}'. Must be one of: {VALID_ROLES}")

        sql = f"UPDATE {self._schema}.Users SET Role = ? WHERE Id = ?"
        self._db.execute(sql, (role, user_id))
        self._db.commit()

        return True


    def set_highlight_intensity(self, user_id: int, intensity: str) -> None:
        """Update a user's highlight intensity preference."""

        valid = {'dim', 'standard', 'bright'}
        if intensity not in valid:
            raise ValueError(f"Invalid intensity '{intensity}'. Must be one of: {valid}")

        sql = f"UPDATE {self._schema}.Users SET HighlightIntensity = ? WHERE Id = ?"
        self._db.execute(sql, (intensity, user_id))
        self._db.commit()


    def list_all(self) -> list[User]:
        """List all users ordered by creation date."""

        sql = f"""
            SELECT Id, AzureObjectId, Email, DisplayName, Role, CreatedAt, LastLoginAt, HighlightIntensity
            FROM {self._schema}.Users
            ORDER BY CreatedAt DESC
        """

        cursor = self._db.execute(sql)
        cols   = [col[0] for col in cursor.description]

        return [self._row_to_user(row) for row in cursor.fetchall()]


    # ── Internal helpers ─────────────────────────────────────────────

    def _find_by_oid(self, azure_oid: str) -> User | None:
        """Look up user by Azure Object ID."""

        sql = f"""
            SELECT Id, AzureObjectId, Email, DisplayName, Role, CreatedAt, LastLoginAt, HighlightIntensity
            FROM {self._schema}.Users
            WHERE AzureObjectId = ?
        """

        cursor = self._db.execute(sql, (azure_oid,))
        row    = cursor.fetchone()

        return self._row_to_user(row) if row else None


    def _find_by_email(self, email: str) -> User | None:
        """Look up user by email address."""

        sql = f"""
            SELECT Id, AzureObjectId, Email, DisplayName, Role, CreatedAt, LastLoginAt, HighlightIntensity
            FROM {self._schema}.Users
            WHERE Email = ?
        """

        cursor = self._db.execute(sql, (email,))
        row    = cursor.fetchone()

        return self._row_to_user(row) if row else None


    def _create(self, azure_oid: str, email: str, display_name: str) -> User:
        """Insert a new user with pending role."""

        sql = f"""
            INSERT INTO {self._schema}.Users (AzureObjectId, Email, DisplayName, LastLoginAt)
            OUTPUT INSERTED.Id, INSERTED.CreatedAt
            VALUES (?, ?, ?, SYSUTCDATETIME())
        """

        cursor = self._db.execute(sql, (azure_oid, email, display_name))
        row    = cursor.fetchone()
        self._db.commit()

        log.info(f"Created new user: {email} (id={row[0]})")

        return User(
            id              = row[0],
            azure_object_id = azure_oid,
            email           = email,
            display_name    = display_name,
            role            = "pending",
            created_at      = row[1],
            last_login_at   = datetime.utcnow(),
        )


    def _update_last_login(self, user_id: int) -> None:
        """Bump LastLoginAt timestamp."""

        sql = f"UPDATE {self._schema}.Users SET LastLoginAt = SYSUTCDATETIME() WHERE Id = ?"
        self._db.execute(sql, (user_id,))
        self._db.commit()


    def _update_oid_and_login(self, user_id: int, azure_oid: str, display_name: str) -> None:
        """Update AzureObjectId and DisplayName for seeded users on first real login."""

        sql = f"""
            UPDATE {self._schema}.Users
            SET AzureObjectId = ?, DisplayName = ?, LastLoginAt = SYSUTCDATETIME()
            WHERE Id = ?
        """

        self._db.execute(sql, (azure_oid, display_name, user_id))
        self._db.commit()

        log.info(f"Updated OID for seeded user id={user_id}")


    @staticmethod
    def _row_to_user(row) -> User:
        """Convert a pyodbc row to a User dataclass."""

        return User(
            id                  = row[0],
            azure_object_id     = row[1],
            email               = row[2],
            display_name        = row[3],
            role                = row[4],
            created_at          = row[5],
            last_login_at       = row[6],
            highlight_intensity = row[7] if len(row) > 7 else 'standard',
        )
