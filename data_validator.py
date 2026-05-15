"""
Data Validator para o Pipeline B3
Valida CNPJs, datas, campos textuais e gera Data Quality Reports.
"""

import re
from datetime import datetime
from typing import Any, Dict, List


# Caracteres de controle que nunca devem aparecer no SQL
CONTROL_CHARS_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')

# Padrões de SQL injection (defesa em profundidade)
SQL_DANGER_PATTERNS = [
    re.compile(r";\s*(DROP|DELETE|TRUNCATE|ALTER|CREATE|INSERT|UPDATE)\s", re.IGNORECASE),
    re.compile(r"EXECUTE\s+IMMEDIATE", re.IGNORECASE),
    re.compile(r"DBMS_SQL", re.IGNORECASE),
    re.compile(r"UTL_(FILE|HTTP|SMTP)", re.IGNORECASE),
]


def validate_cnpj(cnpj: str) -> tuple:
    """Valida CNPJ com dígitos verificadores (módulo 11).
    Retorna (valid: bool, error: str)."""
    if not cnpj:
        return False, "CNPJ vazio"

    digits = ''.join(c for c in cnpj if c.isdigit())
    if len(digits) != 14:
        return False, f"CNPJ com {len(digits)} dígitos (esperado 14)"

    if len(set(digits)) == 1:
        return False, "CNPJ com todos os dígitos iguais"

    # Primeiro dígito verificador
    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum1 = sum(int(digits[i]) * weights1[i] for i in range(12))
    check1 = 0 if (sum1 % 11) < 2 else 11 - (sum1 % 11)
    if int(digits[12]) != check1:
        return False, "Primeiro dígito verificador inválido"

    # Segundo dígito verificador
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum2 = sum(int(digits[i]) * weights2[i] for i in range(13))
    check2 = 0 if (sum2 % 11) < 2 else 11 - (sum2 % 11)
    if int(digits[13]) != check2:
        return False, "Segundo dígito verificador inválido"

    return True, ""


def validate_date(date_str: str) -> tuple:
    """Valida data no formato YYYY-MM-DD. Retorna (valid, error)."""
    if not date_str or not date_str.strip():
        return False, "Data vazia"
    date_str = date_str.strip()
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return False, f"Formato inválido: '{date_str}'"
    y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
    if not (1900 <= y <= 2100):
        return False, f"Ano fora do range: {y}"
    if not (1 <= m <= 12):
        return False, f"Mês inválido: {m}"
    if not (1 <= d <= 31):
        return False, f"Dia inválido: {d}"
    return True, ""


def sanitize_text(text: str) -> str:
    """Remove caracteres de controle de um texto."""
    if not text:
        return text
    return CONTROL_CHARS_RE.sub('', str(text))


def check_sql_safety(text: str) -> tuple:
    """Verifica padrões perigosos de SQL injection. Retorna (safe, pattern_found)."""
    if not text:
        return True, ""
    for pattern in SQL_DANGER_PATTERNS:
        if pattern.search(str(text)):
            return False, pattern.pattern
    return True, ""


def validate_fund(record: dict) -> dict:
    """Validação completa de um registro de fundo.

    Retorna dict com:
      - rejected: bool (se o fundo deve ser excluído do SQL)
      - rejection_reason: str
      - warnings: list de avisos (campo omitido do SQL mas fundo aceito)
      - sanitized: dict de campos que foram sanitizados {campo: valor_limpo}
      - invalid_fields: set de campos que falharam validação (omitir do SQL)
    """
    cnpj = record.get("cnpj_fundo", "")
    result = {
        "rejected": False,
        "rejection_reason": "",
        "warnings": [],
        "sanitized": {},
        "invalid_fields": set(),
    }

    # CNPJ do fundo: se inválido, rejeita o fundo inteiro
    valid, err = validate_cnpj(cnpj)
    if not valid:
        result["rejected"] = True
        result["rejection_reason"] = f"CNPJ fundo inválido: {err}"
        return result

    # CNPJs de participantes: se inválido, apenas omite do SQL
    for field in ("administrador_id", "gestor_id", "escriturador_id", "custodiante_id"):
        value = record.get(field)
        if value:
            valid, err = validate_cnpj(value)
            if not valid:
                result["invalid_fields"].add(field)
                result["warnings"].append(f"{field}: {err}")

    # Datas
    for field in ("data_registro", "data_constituicao", "data_situacao"):
        value = record.get(field)
        if value:
            valid, err = validate_date(value)
            if not valid:
                result["invalid_fields"].add(field)
                result["warnings"].append(f"{field}: {err}")

    # Razão social: tamanho
    razao = record.get("razao_social_final")
    if razao and (len(razao) < 3 or len(razao) > 300):
        result["invalid_fields"].add("razao_social_final")
        result["warnings"].append(f"razao_social_final: tamanho {len(razao)} fora do range 3-300")

    # Sanitização de caracteres de controle em todos os campos texto
    text_fields = [
        "razao_social_final", "razao_social_normalizada",
        "administrador_nome", "gestor_nome", "escriturador_nome", "custodiante_nome",
        "administrador_endereco", "gestor_endereco", "escriturador_endereco", "custodiante_endereco",
        "administrador_email", "gestor_email", "escriturador_email", "custodiante_email",
        "natureza_economica_final", "natureza_juridica_final",
        "diretor_responsavel", "denominacao_social_fundo", "denominacao_social_classe",
    ]
    for field in text_fields:
        value = record.get(field)
        if value:
            cleaned = sanitize_text(value)
            if cleaned != value:
                result["sanitized"][field] = cleaned
                result["warnings"].append(f"{field}: caracteres de controle removidos")
            # SQL safety
            safe, pattern = check_sql_safety(value)
            if not safe:
                result["invalid_fields"].add(field)
                result["warnings"].append(f"{field}: padrão SQL perigoso detectado")

    return result


def generate_quality_report(results: list, validations: list, guardrails: dict = None) -> dict:
    """Gera relatório de qualidade de dados completo.

    Args:
        results: lista de registros consolidados
        validations: lista de dicts retornados por validate_fund()
        guardrails: dict com contadores dos guardrails aplicados
    """
    if guardrails is None:
        guardrails = {}

    total = len(results)
    rejected = sum(1 for v in validations if v["rejected"])

    # Contadores de validação
    cnpj_fundo_invalido = sum(1 for v in validations if v["rejected"])
    cnpj_part_invalidos = {}
    for field in ("administrador_id", "gestor_id", "escriturador_id", "custodiante_id"):
        cnpj_part_invalidos[field] = sum(1 for v in validations if field in v["invalid_fields"])

    data_invalidas = 0
    for v in validations:
        for field in ("data_registro", "data_constituicao", "data_situacao"):
            if field in v["invalid_fields"]:
                data_invalidas += 1

    chars_sanitizados = sum(len(v["sanitized"]) for v in validations)

    # Cobertura por campo
    coverage_fields = [
        "razao_social_final", "administrador_id", "administrador_nome",
        "gestor_id", "gestor_nome", "natureza_economica_final",
        "natureza_juridica_final", "custodiante_id", "escriturador_id",
        "diretor_responsavel", "codigo_cvm", "data_registro", "data_constituicao",
    ]
    cobertura = {}
    for field in coverage_fields:
        preenchido = sum(1 for r in results if r.get(field) and str(r[field]).strip())
        vazio = total - preenchido
        pct = round(preenchido / total * 100, 1) if total > 0 else 0
        cobertura[field] = {"preenchido": preenchido, "vazio": vazio, "pct": f"{pct}%"}

    # Distribuição de gaps
    gap_dist = {"0_gaps": 0, "1_gap": 0, "2_gaps": 0, "3+_gaps": 0}
    for r in results:
        n = len(r.get("gaps", []))
        if n == 0:
            gap_dist["0_gaps"] += 1
        elif n == 1:
            gap_dist["1_gap"] += 1
        elif n == 2:
            gap_dist["2_gaps"] += 1
        else:
            gap_dist["3+_gaps"] += 1

    # Listas detalhadas de problemas
    rejeitados_detalhe = []
    participantes_invalidos_detalhe = {"administrador_id": [], "gestor_id": [], "escriturador_id": [], "custodiante_id": []}

    for r, v in zip(results, validations):
        cnpj = r.get("cnpj_fundo", "")
        nome = (r.get("razao_social_final") or "")[:80]

        if v["rejected"]:
            rejeitados_detalhe.append({
                "cnpj_fundo": cnpj,
                "razao_social": nome,
                "motivo": v.get("rejection_reason", ""),
            })

        for field in ("administrador_id", "gestor_id", "escriturador_id", "custodiante_id"):
            if field in v.get("invalid_fields", set()):
                participantes_invalidos_detalhe[field].append({
                    "cnpj_fundo": cnpj,
                    "razao_social": nome,
                    "valor_invalido": r.get(field, ""),
                    "nome_participante": (r.get(field.replace("_id", "_nome")) or "")[:60],
                })

    return {
        "timestamp": datetime.now().isoformat(),
        "total_processados": total,
        "validos_para_sql": total - rejected,
        "rejeitados_total": rejected,
        "validacao": {
            "cnpj_fundo_invalido": cnpj_fundo_invalido,
            "cnpj_administrador_invalido": cnpj_part_invalidos.get("administrador_id", 0),
            "cnpj_gestor_invalido": cnpj_part_invalidos.get("gestor_id", 0),
            "cnpj_escriturador_invalido": cnpj_part_invalidos.get("escriturador_id", 0),
            "cnpj_custodiante_invalido": cnpj_part_invalidos.get("custodiante_id", 0),
            "data_formato_invalido": data_invalidas,
            "caracteres_controle_sanitizados": chars_sanitizados,
        },
        "rejeitados_detalhes": rejeitados_detalhe,
        "participantes_invalidos_detalhes": {
            k: v for k, v in participantes_invalidos_detalhe.items() if v
        },
        "guardrails": guardrails,
        "cobertura_por_campo": cobertura,
        "distribuicao_gaps": gap_dist,
    }

