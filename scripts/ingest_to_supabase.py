"""
Supabase ingestion with IPv4 resolution fallback and jsonb-safe inserts.

- Reads CSVs in data/bronze/<fund_slug>/portfolio_*.csv and cashflow_*.csv
- Inserts rows as jsonb into portfolio_normalized and cashflow_normalized
- Logs errors into ingestion_errors

Requires:
  SUPABASE_DB_URL (postgresql://postgres:<pass>@<host>:5432/postgres)
Optional:
  INGEST_BATCH_SIZE (default 500)
"""
from __future__ import annotations

import csv
import os
import socket
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse

import psycopg
from psycopg import sql
from psycopg.types.json import Json

BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "500"))


def ensure_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists portfolio_normalized (
                id bigserial primary key,
                ingested_at timestamptz not null default now(),
                fund_slug text not null,
                report_date date,
                row_number int,
                data jsonb not null
            );
            create index if not exists idx_portfolio_fund_date on portfolio_normalized (fund_slug, report_date);

            create table if not exists cashflow_normalized (
                id bigserial primary key,
                ingested_at timestamptz not null default now(),
                fund_slug text not null,
                range_label text,
                row_number int,
                data jsonb not null
            );
            create index if not exists idx_cashflow_fund_range on cashflow_normalized (fund_slug, range_label);

            create table if not exists ingestion_errors (
                id bigserial primary key,
                created_at timestamptz not null default now(),
                fund_slug text,
                report_type text,
                file_path text,
                error_message text
            );
            """
        )
    conn.commit()


def read_csv_as_dicts(path: Path) -> List[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def insert_batch(conn: psycopg.Connection, table: str, rows: List[Tuple]) -> None:
    if not rows:
        return
    query = sql.SQL(
        "insert into {table} (fund_slug, report_date, range_label, row_number, data) values (%s, %s, %s, %s, %s)"
    ).format(table=sql.Identifier(table))
    rows_prepared = [(f, rd, rl, rn, Json(d)) for (f, rd, rl, rn, d) in rows]  # wrap dict -> jsonb
    with conn.cursor() as cur:
        cur.executemany(query, rows_prepared)
    conn.commit()


def log_error(conn: psycopg.Connection, fund_slug: str, report_type: str, file_path: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "insert into ingestion_errors (fund_slug, report_type, file_path, error_message) values (%s,%s,%s,%s)",
            (fund_slug, report_type, str(file_path), message[:2000]),
        )
    conn.commit()


def process_portfolios(conn: psycopg.Connection, fund_slug: str, files: List[Path]) -> None:
    for path in files:
        try:
            rows = read_csv_as_dicts(path)
            if not rows:
                log_error(conn, fund_slug, "portfolio", str(path), "empty file")
                continue
            report_date_str = path.stem.replace("portfolio_", "")
            try:
                report_date = datetime.fromisoformat(report_date_str).date()
            except Exception:
                report_date = None
            batch: List[Tuple] = []
            for idx, row in enumerate(rows):
                batch.append((fund_slug, report_date, None, idx, row))
                if len(batch) >= BATCH_SIZE:
                    insert_batch(conn, "portfolio_normalized", batch)
                    batch.clear()
            if batch:
                insert_batch(conn, "portfolio_normalized", batch)
        except Exception as exc:
            log_error(conn, fund_slug, "portfolio", str(path), str(exc))


def process_cashflows(conn: psycopg.Connection, fund_slug: str, files: List[Path]) -> None:
    for path in files:
        try:
            rows = read_csv_as_dicts(path)
            if not rows:
                log_error(conn, fund_slug, "cashflow", str(path), "empty file")
                continue
            range_label = path.stem.replace("cashflow_", "")
            batch: List[Tuple] = []
            for idx, row in enumerate(rows):
                batch.append((fund_slug, None, range_label, idx, row))
                if len(batch) >= BATCH_SIZE:
                    insert_batch(conn, "cashflow_normalized", batch)
                    batch.clear()
            if batch:
                insert_batch(conn, "cashflow_normalized", batch)
        except Exception as exc:
            log_error(conn, fund_slug, "cashflow", str(path), str(exc))


def resolve_ipv4(host: str, port: int) -> str:
    for family, _, _, _, sockaddr in socket.getaddrinfo(host, port, family=socket.AF_INET, type=socket.SOCK_STREAM):
        if family == socket.AF_INET:
            return sockaddr[0]
    return ""


def connect_with_ipv4(db_url: str) -> psycopg.Connection:
    parsed = urlparse(db_url)
    host = parsed.hostname
    port = parsed.port or 5432
    user = parsed.username
    passwd = parsed.password
    dbname = parsed.path.lstrip("/") or "postgres"
    ipv4 = resolve_ipv4(host, port)
    if ipv4:
        return psycopg.connect(
            dbname=dbname,
            user=user,
            password=passwd,
            hostaddr=ipv4,
            port=port,
            sslmode="require",
            connect_timeout=10,
        )
    # fallback para URL original se s� houver IPv6 e for roteado
    return psycopg.connect(db_url, connect_timeout=10)


def main() -> None:
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise SystemExit("SUPABASE_DB_URL is required")
    conn = connect_with_ipv4(db_url)

    ensure_tables(conn)
    for fund_dir in BRONZE_DIR.iterdir():
        if not fund_dir.is_dir():
            continue
        fund_slug = fund_dir.name
        portfolio_files = sorted(fund_dir.glob("portfolio_*.csv"))
        cashflow_files = sorted(fund_dir.glob("cashflow_*.csv"))
        if not portfolio_files and not cashflow_files:
            continue
        process_portfolios(conn, fund_slug, portfolio_files)
        process_cashflows(conn, fund_slug, cashflow_files)
    conn.close()


if __name__ == "__main__":
    main()
