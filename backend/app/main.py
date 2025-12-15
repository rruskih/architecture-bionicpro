import os
from datetime import date, datetime
from typing import Any, Optional

import asyncpg
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
from pydantic import BaseModel


OLAP_DSN = os.getenv(
    "OLAP_DSN", "postgresql://olap_user:olap_password@olap_db:5432/olap_db"
)

KEYCLOAK_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER", "http://keycloak:8080/realms/reports-realm"
)
KEYCLOAK_JWKS_URL = os.getenv(
    "KEYCLOAK_JWKS_URL",
    f"{KEYCLOAK_ISSUER}/protocol/openid-connect/certs",
)
KEYCLOAK_AUDIENCE = os.getenv("KEYCLOAK_AUDIENCE")  # Optional audience check
CLIENT_ID_CLAIM = os.getenv("CLIENT_ID_CLAIM", "preferred_username")

app = FastAPI(title="Reports API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Report(BaseModel):
    client_id: str
    ds: date
    segment: Optional[str]
    country: Optional[str]
    plan: Optional[str]
    events_cnt: int
    errors_cnt: int
    avg_latency_ms: float
    p95_latency_ms: float
    sessions_cnt: int
    last_event_at: Optional[datetime]
    loaded_at: datetime


async def get_pool() -> asyncpg.pool.Pool:
    pool = getattr(app.state, "pool", None)
    if pool is None:
        app.state.pool = await asyncpg.create_pool(
            dsn=OLAP_DSN, min_size=1, max_size=5, command_timeout=30
        )
    return app.state.pool


async def get_jwks() -> dict[str, Any]:
    jwks = getattr(app.state, "jwks", None)
    if jwks is None:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(KEYCLOAK_JWKS_URL)
            resp.raise_for_status()
            jwks = resp.json()
            app.state.jwks = jwks
    return jwks


@app.on_event("startup")
async def on_startup() -> None:
    await get_pool()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    pool = getattr(app.state, "pool", None)
    if pool:
        await pool.close()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def fetch_report(
    pool: asyncpg.pool.Pool, client_id: str, ds: Optional[date]
) -> Report:
    query = """
        SELECT
            client_id,
            ds,
            segment,
            country,
            plan,
            events_cnt,
            errors_cnt,
            avg_latency_ms,
            p95_latency_ms,
            sessions_cnt,
            last_event_at,
            loaded_at
        FROM dm_client_telemetry
        WHERE client_id = $1
        {ds_filter}
        ORDER BY ds DESC
        LIMIT 1
    """
    ds_filter = "AND ds = $2" if ds else ""
    sql = query.format(ds_filter=ds_filter)
    params = (client_id, ds) if ds else (client_id,)
    row = await pool.fetchrow(sql, *params)
    if not row:
        raise HTTPException(status_code=404, detail="Report not found")
    return Report(**dict(row))


async def decode_and_validate_token(token: str) -> dict[str, Any]:
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    unverified_header = jwt.get_unverified_header(token)
    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="Token header missing kid")

    jwks = await get_jwks()
    key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
    if not key:
        # refresh jwks once if key not found
        app.state.jwks = None
        jwks = await get_jwks()
        key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
        if not key:
            raise HTTPException(status_code=401, detail="Signing key not found")

    options = {"verify_aud": bool(KEYCLOAK_AUDIENCE)}
    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=[key.get("alg", "RS256")],
            audience=KEYCLOAK_AUDIENCE if KEYCLOAK_AUDIENCE else None,
            issuer=KEYCLOAK_ISSUER,
            options=options,
        )
    except JWTError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}") from exc

    return claims


async def get_current_client_id(
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authorization header is missing or invalid")
    token = authorization.split(" ", 1)[1]

    claims = await decode_and_validate_token(token)
    client_id = claims.get(CLIENT_ID_CLAIM) or claims.get("client_id") or claims.get("sub")
    if not client_id:
        raise HTTPException(status_code=403, detail="client_id claim is missing")
    return client_id


def resolve_requested_date(ds: Optional[date]) -> date:
    """
    Airflow генерирует витрину за предыдущий день.
    Разрешаем запрашивать только даты, которые уже обработаны (<= вчера).
    Если дата не указана — используем вчера.
    """
    today = date.today()
    processed_cutoff = today - timedelta(days=1)
    effective_ds = ds or processed_cutoff
    if effective_ds > processed_cutoff:
        raise HTTPException(
            status_code=400,
            detail="Запрошенная дата еще не обработана витриной (доступно только до вчера).",
        )
    return effective_ds


@app.get("/reports", response_model=Report, summary="Получить отчет по клиенту")
async def get_report(
    ds: Optional[date] = Query(default=None, description="Дата среза (YYYY-MM-DD)"),
    pool: asyncpg.pool.Pool = Depends(get_pool),
    client_id: str = Depends(get_current_client_id),
) -> Report:
    effective_ds = resolve_requested_date(ds)
    return await fetch_report(pool, client_id, effective_ds)
