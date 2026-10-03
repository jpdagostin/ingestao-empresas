"""
PostgreSQL schema definition and streaming COPY loader.

Provides DDL for UNLOGGED high-performance bulk loading, TSV serialization,
and StringIteratorIO to stream directly into PostgreSQL via cursor.copy_expert.
"""

from __future__ import annotations

import io
from typing import Any, Iterable, Iterator, Tuple


def get_ddl_statement(table_name: str) -> str:
    """Return SQL statements to drop and recreate the table as UNLOGGED for bulk loading."""
    # Quoted identifier to support hyphens in participant names
    safe_table = table_name.replace('"', '""')
    return f"""
    DROP TABLE IF EXISTS public."{safe_table}" CASCADE;
    CREATE UNLOGGED TABLE public."{safe_table}" (
        cnpj_basico VARCHAR(8) NOT NULL,
        razao_social VARCHAR NOT NULL,
        natureza_juridica VARCHAR(4) NOT NULL,
        qualificacao_responsavel VARCHAR NOT NULL,
        capital_social DOUBLE PRECISION NOT NULL,
        porte_codigo VARCHAR(2) NOT NULL,
        porte_descricao VARCHAR NOT NULL,
        ente_federativo VARCHAR,
        capital_social_faixa VARCHAR NOT NULL,
        is_mei BOOLEAN NOT NULL,
        natureza_juridica_grupo VARCHAR NOT NULL,
        ente_federativo_presente BOOLEAN NOT NULL,
        data_processamento TIMESTAMP NOT NULL
    );
    """


def get_finalize_statement(table_name: str) -> str:
    """Return SQL statement to alter table back to LOGGED and analyze statistics."""
    safe_table = table_name.replace('"', '""')
    return f"""
    ALTER TABLE public."{safe_table}" SET LOGGED;
    ANALYZE public."{safe_table}";
    """


def serialize_row_tsv(row: Tuple[Any, ...]) -> str:
    """Serialize a transformed row to Tab-Separated Values (TSV) for COPY FROM STDIN."""
    parts = []
    for val in row:
        if val is None:
            parts.append(r"\N")
        elif isinstance(val, bool):
            parts.append("t" if val else "f")
        elif isinstance(val, float):
            parts.append(f"{val:.2f}")
        else:
            s = str(val).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ")
            parts.append(s)
    return "\t".join(parts) + "\n"


class StringIteratorIO(io.TextIOBase):
    """File-like stream reader wrapping a string generator.

    Allows psycopg2 cursor.copy_expert to pull data in batches
    without buffering the entire dataset in RAM or writing to disk.
    """

    def __init__(self, line_iterator: Iterator[str]):
        self._iter = line_iterator
        self._leftover = ""

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1) -> str:
        if size is None or size < 0:
            return self._leftover + "".join(self._iter)

        chunks = [self._leftover]
        total_len = len(self._leftover)
        self._leftover = ""

        try:
            while total_len < size:
                chunk = next(self._iter)
                chunks.append(chunk)
                total_len += len(chunk)
        except StopIteration:
            pass

        full = "".join(chunks)
        if len(full) > size:
            self._leftover = full[size:]
            return full[:size]
        return full
