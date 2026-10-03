"""
Streaming reader for raw zipped CSV files.

Reads directly from .zip archive streams without disk extraction or loading entire files into memory.
Parses RFC 4180 CSV with ';' delimiter and '"' quotes to handle quotes containing embedded semicolons.
"""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import Generator, List


def iter_raw_rows(zip_path: Path) -> Generator[List[str], None, None]:
    """Stream rows from the first CSV-like member inside a .zip archive.

    Decodes bytes on-the-fly using ISO-8859-1 (Latin-1) into Unicode strings.
    """
    with zipfile.ZipFile(zip_path, mode="r") as zf:
        namelist = zf.namelist()
        if not namelist:
            return

        csv_members = [
            name for name in namelist
            if name.endswith((".EMPRECSV", ".csv", ".txt")) or "." not in name
        ]
        target_name = csv_members[0] if csv_members else namelist[0]

        with zf.open(target_name, mode="r") as raw_stream:
            text_stream = io.TextIOWrapper(
                raw_stream,
                encoding="iso-8859-1",
                errors="strict",
                newline="",
            )

            reader = csv.reader(
                text_stream,
                delimiter=";",
                quotechar='"',
                doublequote=True,
                skipinitialspace=False,
            )

            for row in reader:
                if row:
                    yield row
