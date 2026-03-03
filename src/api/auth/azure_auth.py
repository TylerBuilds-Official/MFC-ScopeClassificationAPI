"""
Azure AD JWT validation via manual JWKS verification.
Validates tokens and extracts user claims.
"""

import os
import logging

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from jwt import PyJWKClient


log = logging.getLogger(__name__)

security     = HTTPBearer()
_jwks_client = None


def _get_config() -> tuple[str, str]:
    """Lazy-load Azure config after .env is loaded by lifespan."""

    client_id = os.getenv("AZURE_CLIENT_ID", "")
    tenant_id = os.getenv("AZURE_TENANT_ID", "")

    return client_id, tenant_id


def _get_jwks_client() -> PyJWKClient:
    """Lazy-init JWKS client on first token validation."""

    global _jwks_client

    if _jwks_client is None:
        _, tenant_id = _get_config()
        jwks_uri     = f"https://login.microsoftonline.com/{tenant_id}/discovery/keys"
        _jwks_client = PyJWKClient(jwks_uri)

    return _jwks_client


async def validate_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Validate Azure AD JWT and return claims dict."""

    token = credentials.credentials

    client_id, tenant_id = _get_config()
    jwks                  = _get_jwks_client()

    try:
        signing_key = jwks.get_signing_key_from_jwt(token)

        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=f"api://{client_id}",
            options={
                "verify_signature": True,
                "verify_exp":       True,
                "verify_nbf":       True,
                "verify_iat":       True,
                "verify_aud":       True,
                "verify_iss":       False,
            },
        )

        # Manually verify issuer is from our tenant
        issuer         = claims.get("iss", "")
        valid_issuers  = [
            f"https://sts.windows.net/{tenant_id}/",
            f"https://login.microsoftonline.com/{tenant_id}/v2.0",
        ]

        if issuer not in valid_issuers:
            log.warning(f"Invalid issuer: {issuer}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token issuer",
            )

        log.info(f"Token validated: {claims.get('upn')} ({claims.get('oid')})")

        return claims

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except jwt.InvalidAudienceError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token audience",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except HTTPException:
        raise

    except Exception as e:
        log.error(f"Token validation failed: {type(e).__name__}: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
