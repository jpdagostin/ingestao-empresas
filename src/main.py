"""
Main entrypoint for Ingestão no Limite pipeline.

Executes streaming decompression, business enrichment, and direct PostgreSQL COPY loading.
Respects 1 GB RAM / 2 CPUs limits by streaming rows in memory without buffering.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

# Robust sys.path resolution for both `python src/main.py` and `python -m src.main`
_CURRENT_DIR = Path(__file__).resolve().parent
_PARENT_DIR = _CURRENT_DIR.parent
for _p in (str(_CURRENT_DIR), str(_PARENT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import psycopg2

try:
    from src.loader import (
        StringIteratorIO,
        get_ddl_statement,
        get_finalize_statement,
        serialize_row_tsv,
    )
    from src.reader import iter_raw_rows
    from src.transformer import transform_row
except ImportError:
    from loader import (  # type: ignore
        StringIteratorIO,
        get_ddl_statement,
        get_finalize_statement,
        serialize_row_tsv,
    )
    from reader import iter_raw_rows  # type: ignore
    from transformer import transform_row  # type: ignore


def stream_transformed_tsv(zip_path: Path, timestamp_str: str) -> Iterator[str]:
    """Generator streaming TSV formatted rows from a zip file."""
    for raw_row in iter_raw_rows(zip_path):
        transformed = transform_row(raw_row, timestamp_str)
        yield serialize_row_tsv(transformed)


def run_pipeline() -> None:
    start_total = time.time()

    # 1. Read environment variables
    participante = os.environ.get("PARTICIPANTE", "jpdagostin")
    pg_table = os.environ.get("PG_TABLE", f"{participante}_empresas")
    pg_host = os.environ.get("PG_HOST", "postgres_db")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    pg_user = os.environ.get("PG_USER", "homelab_postgres")
    pg_password = os.environ.get("PG_PASSWORD", "")
    pg_db = os.environ.get("PG_DB", "db_empresas")

    raw_data_dir = os.environ.get("DATA_DIR", "/data")
    data_dir = Path(raw_data_dir)

    print("=" * 60)
    print("  INGESTÃO NO LIMITE — PIPELINE DE INGESTÃO")
    print("=" * 60)
    print(f"Participante   : {participante}")
    print(f"Tabela Destino : public.{pg_table}")
    print(f"Postgres       : {pg_user}@{pg_host}:{pg_port}/{pg_db}")
    print(f"Diretório Dados: {data_dir}")

    # 2. Verify data directory and find .zip files
    if not data_dir.exists():
        print(f"[ERRO] Diretório de dados {data_dir} não encontrado.", file=sys.stderr)
        sys.exit(1)

    zip_files = sorted(data_dir.glob("*.zip"))
    print(f"Arquivos .zip encontrados: {len(zip_files)}")
    for z in zip_files:
        print(f"  - {z.name}")

    if not zip_files:
        print("[AVISO] Nenhum arquivo .zip encontrado para processar.")
        sys.exit(0)

    # 3. Connect to PostgreSQL
    print("\n[*] Conectando ao PostgreSQL...")
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        dbname=pg_db,
    )
    conn.autocommit = False

    try:
        # 4. Prepare DDL (UNLOGGED table for minimal WAL overhead during ingestion)
        with conn.cursor() as cur:
            print("[*] Aplicando DDL otimizado...")
            cur.execute("SET LOCAL synchronous_commit = OFF;")
            cur.execute(get_ddl_statement(pg_table))
        conn.commit()

        timestamp_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        safe_table = pg_table.replace('"', '""')
        copy_sql = f'COPY public."{safe_table}" FROM STDIN WITH (FORMAT text, DELIMITER E\'\\t\', NULL E\'\\\\N\')'

        total_records = 0

        # 5. Ingest each zip in streaming mode
        print("\n[*] Iniciando ingestão streaming via COPY FROM STDIN...")
        for idx, zip_path in enumerate(zip_files, start=1):
            start_zip = time.time()
            count_zip = 0

            # Count rows on the fly while streaming to avoid in-memory accumulation
            def counting_generator():
                nonlocal count_zip
                for line in stream_transformed_tsv(zip_path, timestamp_str):
                    count_zip += 1
                    yield line

            stream_io = StringIteratorIO(counting_generator())

            with conn.cursor() as cur:
                cur.execute("SET LOCAL synchronous_commit = OFF;")
                cur.copy_expert(copy_sql, stream_io)
            conn.commit()

            elapsed_zip = time.time() - start_zip
            rate = count_zip / elapsed_zip if elapsed_zip > 0 else 0
            total_records += count_zip
            print(
                f"  [{idx:02d}/{len(zip_files):02d}] {zip_path.name:<20} : "
                f"{count_zip:>10,} linhas em {elapsed_zip:>6.1f}s ({rate:>8.0f} linhas/s)"
            )

        # 6. Finalize table (SET LOGGED + ANALYZE)
        print("\n[*] Finalizando tabela: alterando para LOGGED e executando ANALYZE...")
        with conn.cursor() as cur:
            cur.execute(get_finalize_statement(pg_table))
        conn.commit()

        total_elapsed = time.time() - start_total
        overall_rate = total_records / total_elapsed if total_elapsed > 0 else 0

        print("\n" + "=" * 60)
        print("  INGESTÃO CONCLUÍDA COM SUCESSO!")
        print("=" * 60)
        print(f"Total de registros inseridos : {total_records:,}")
        print(f"Tempo total de execução      : {total_elapsed:.2f} s ({total_elapsed/60:.2f} min)")
        print(f"Throughput médio sustentado  : {overall_rate:.0f} linhas/s")
        print("=" * 60)

    except Exception as exc:
        conn.rollback()
        print(f"\n[ERRO CRÍTICO] Falha durante a ingestão: {exc}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
