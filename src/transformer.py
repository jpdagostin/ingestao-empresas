"""
Business transformation and contract enforcement module.

Enriches the 7 source columns from Receita Federal CSV into the 13 required schema columns
without dropping any rows (pure ELT philosophy).
"""

from __future__ import annotations

from typing import Any, List, Tuple

PORTE_MAP: dict[str, str] = {
    "00": "NÃO INFORMADO",
    "01": "MICRO EMPRESA",
    "03": "EMPRESA DE PEQUENO PORTE",
    "05": "DEMAIS",
}

NATUREZA_GRUPO_MAP: dict[str, str] = {
    "1": "ADMINISTRAÇÃO PÚBLICA",
    "2": "ENTIDADES EMPRESARIAIS",
    "3": "ENTIDADES SEM FINS LUCRATIVOS",
    "4": "PESSOAS FÍSICAS",
    "5": "ORGANIZAÇÕES INTERNACIONAIS",
}


def transform_row(raw: List[str], timestamp_str: str) -> Tuple[Any, ...]:
    """Transform and enrich a single 7-column raw row into the 13-column target schema.

    Expected raw indices:
      [0] cnpj_basico
      [1] razao_social
      [2] natureza_juridica
      [3] qualificacao_responsavel
      [4] capital_social
      [5] porte_codigo
      [6] ente_federativo
    """
    # 1. cnpj_basico: exactly 8 digits with leading zeros
    cnpj_basico = raw[0].strip().zfill(8)

    # 2. razao_social: UPPER, trimmed, NOT NULL (empty string '' preserved)
    razao_raw = raw[1].strip()
    razao_social = razao_raw.upper()

    # 3. natureza_juridica: exactly 4 digits with leading zeros
    natureza_juridica = raw[2].strip().zfill(4)

    # 4. qualificacao_responsavel: NOT NULL and not empty
    qualificacao_responsavel = raw[3].strip() or "0"

    # 5. capital_social: parse Brazilian comma decimal to float
    cap_raw = raw[4].strip().replace(",", ".")
    try:
        capital_social = float(cap_raw) if cap_raw else 0.0
    except ValueError:
        capital_social = 0.0

    # 6. porte_codigo: normalize empty string to '00'
    porte_raw = raw[5].strip()
    if not porte_raw or porte_raw not in PORTE_MAP:
        porte_codigo = "00"
    else:
        porte_codigo = porte_raw

    # 7. porte_descricao: exact lookup
    porte_descricao = PORTE_MAP[porte_codigo]

    # 8. ente_federativo: empty string becomes None (NULL)
    ente_raw = raw[6].strip()
    ente_federativo = ente_raw if ente_raw else None

    # 9. capital_social_faixa: inclusive upper boundaries
    if capital_social == 0.0:
        capital_social_faixa = "SEM CAPITAL"
    elif capital_social <= 1000.0:
        capital_social_faixa = "ATÉ 1K"
    elif capital_social <= 10000.0:
        capital_social_faixa = "1K A 10K"
    elif capital_social <= 100000.0:
        capital_social_faixa = "10K A 100K"
    elif capital_social <= 1000000.0:
        capital_social_faixa = "100K A 1M"
    else:
        capital_social_faixa = "ACIMA DE 1M"

    # 10. is_mei: true iff razao_social ends in 11 digits
    is_mei = len(razao_social) >= 11 and razao_social[-11:].isdigit()

    # 11. natureza_juridica_grupo: Concla IBGE 1st digit mapping
    primeiro_digito = natureza_juridica[0] if natureza_juridica else ""
    natureza_juridica_grupo = NATUREZA_GRUPO_MAP.get(primeiro_digito, "OUTROS")

    # 12. ente_federativo_presente: true if not null and not empty
    ente_federativo_presente = ente_federativo is not None and len(ente_federativo) > 0

    # 13. data_processamento: ingestion lineage timestamp
    data_processamento = timestamp_str

    return (
        cnpj_basico,
        razao_social,
        natureza_juridica,
        qualificacao_responsavel,
        capital_social,
        porte_codigo,
        porte_descricao,
        ente_federativo,
        capital_social_faixa,
        is_mei,
        natureza_juridica_grupo,
        ente_federativo_presente,
        data_processamento,
    )
