from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel

from src.config import settings


security = HTTPBearer(auto_error=False)


class TokenData(BaseModel):
    user_id: int
    role: Optional[str] = None


@dataclass(frozen=True)
class InternalAuthContext:
    source: str = "internal"


def verify_internal_api_key(
    x_internal_key: str | None = Header(default=None, alias="X-INTERNAL-KEY"),
    x_internal_api_key: str | None = Header(default=None, alias="X-Internal-Api-Key"),
) -> str:
    expected_key = str(settings.INTERNAL_API_KEY or "").strip()
    incoming_key = str(x_internal_key or x_internal_api_key or "").strip()

    if not expected_key or expected_key == "change-me-in-env":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal API key is not configured",
        )

    if not incoming_key or incoming_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized internal access",
        )

    return incoming_key


require_internal_api_key = verify_internal_api_key


def verify_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> TokenData:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = str(credentials.credentials or "").strip()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Empty authorization token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    secret = str(settings.JWT_SECRET or "").strip()

    if not secret or secret == "change-me-in-env":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT secret is not configured",
        )

    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=[settings.JWT_ALGORITHM],
            options={
                "verify_aud": False,
            },
        )

        user_id = payload.get("user_id") or payload.get("sub")

        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return TokenData(
            user_id=int(user_id),
            role=payload.get("role"),
        )

    except (JWTError, ValueError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def optional_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> TokenData | None:
    if credentials is None:
        return None

    return verify_token(credentials)