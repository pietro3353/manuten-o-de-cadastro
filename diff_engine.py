"""
Diff Engine para o Pipeline B3
Compara duas execuções e gera relatórios de delta.
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Campos comparados entre execuções
DIFF_FIELDS = [
    "razao_social_final",
    "administrador_id", "administrador_nome",
    "administrador_endereco", "administrador_telefones", "administrador_email",
    "gestor_id", "gestor_nome",
    "gestor_endereco", "gestor_telefones", "gestor_email",
    "escriturador_id", "escriturador_nome",
    "escriturador_endereco", "escriturador_telefones", "escriturador_email",
    "custodiante_id", "custodiante_nome",
    "custodiante_endereco", "custodiante_telefones", "custodiante_email",
    "natureza_economica_final", "natureza_juridica_final",
    "tipo_fundo", "tipo_classe",
    "situacao_final", "data_situacao",
    "data_registro", "data_constituicao",
    "diretor_responsavel",
    "codigo_cvm",
    "denominacao_social_fundo", "denominacao_social_classe",
]


def archive_previous(output_path: str, archive_base: str = "archive") -> Optional[str]:
    """Arquiva o resultado anterior antes de sobrescrever.

    Retorna o caminho do arquivo arquivado, ou None se não existia anterior.
    """
    if not os.path.exists(output_path):
        return None

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    archive_dir = os.path.join(os.path.dirname(output_path) or ".", archive_base, timestamp)
    os.makedirs(archive_dir, exist_ok=True)

    basename = os.path.basename(output_path)
    dest = os.path.join(archive_dir, basename)
    shutil.copy2(output_path, dest)

    # Também arquiva o SQL se existir
    sql_path = os.path.join(os.path.dirname(output_path) or ".", "update_fundos.sql")
    if os.path.exists(sql_path):
        shutil.copy2(sql_path, os.path.join(archive_dir, "update_fundos.sql"))

    return dest


def load_previous_records(output_path: str) -> Optional[list]:
    """Carrega os registros da execução anterior.

    Retorna None se o arquivo não existir ou for inválido.
    """
    if not os.path.exists(output_path):
        return None

    try:
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("records", [])
    except (json.JSONDecodeError, OSError):
        return None


def compute_diff(previous_records: list, current_records: list) -> dict:
    """Compara dois conjuntos de registros campo-a-campo.

    Args:
        previous_records: registros da execução anterior
        current_records: registros da execução atual

    Returns:
        Relatório de diff completo
    """
    prev_by_cnpj = {r["cnpj_fundo"]: r for r in previous_records if r.get("cnpj_fundo")}
    curr_by_cnpj = {r["cnpj_fundo"]: r for r in current_records if r.get("cnpj_fundo")}

    prev_cnpjs = set(prev_by_cnpj.keys())
    curr_cnpjs = set(curr_by_cnpj.keys())

    novos = curr_cnpjs - prev_cnpjs
    removidos = prev_cnpjs - curr_cnpjs
    comuns = prev_cnpjs & curr_cnpjs

    alterados = []
    campos_count = {}

    for cnpj in comuns:
        prev_r = prev_by_cnpj[cnpj]
        curr_r = curr_by_cnpj[cnpj]

        changes = {}
        for field in DIFF_FIELDS:
            pv = str(prev_r.get(field) or "")
            cv = str(curr_r.get(field) or "")
            if pv != cv:
                changes[field] = {"anterior": pv or None, "novo": cv or None}
                campos_count[field] = campos_count.get(field, 0) + 1

        if changes:
            alterados.append({
                "cnpj_fundo": cnpj,
                "razao_social": curr_r.get("razao_social_final", ""),
                "total_campos_alterados": len(changes),
                "campos_alterados": changes,
            })

    alterados.sort(key=lambda x: x["total_campos_alterados"], reverse=True)
    campos_ranking = dict(sorted(campos_count.items(), key=lambda x: x[1], reverse=True))

    return {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "total_anterior": len(prev_by_cnpj),
            "total_atual": len(curr_by_cnpj),
        },
        "resumo": {
            "fundos_novos": len(novos),
            "fundos_removidos": len(removidos),
            "fundos_alterados": len(alterados),
            "fundos_inalterados": len(comuns) - len(alterados),
            "campos_mais_alterados": campos_ranking,
        },
        "detalhes": {
            "novos": sorted(list(novos))[:500],
            "removidos": sorted(list(removidos))[:500],
            "alterados": alterados[:1000],
            "alterados_total": len(alterados),
        },
    }


def get_changed_cnpjs(diff_report: dict) -> Set[str]:
    """Retorna o set de CNPJs novos ou alterados (para SQL delta)."""
    changed = set()
    for item in diff_report.get("detalhes", {}).get("alterados", []):
        changed.add(item["cnpj_fundo"])
    for cnpj in diff_report.get("detalhes", {}).get("novos", []):
        changed.add(cnpj)
    return changed


def generate_summary_text(diff_report: dict) -> str:
    """Gera resumo legível do diff."""
    r = diff_report.get("resumo", {})
    m = diff_report.get("meta", {})

    lines = [
        "=" * 60,
        "  RELATÓRIO DE ALTERAÇÕES — PIPELINE B3",
        f"  Gerado em: {m.get('timestamp', 'N/A')}",
        "=" * 60,
        "",
        f"  Total anterior:    {m.get('total_anterior', 0):>8,}",
        f"  Total atual:       {m.get('total_atual', 0):>8,}",
        "",
        f"  Fundos novos:      {r.get('fundos_novos', 0):>8,}",
        f"  Fundos removidos:  {r.get('fundos_removidos', 0):>8,}",
        f"  Fundos alterados:  {r.get('fundos_alterados', 0):>8,}",
        f"  Fundos inalterados:{r.get('fundos_inalterados', 0):>8,}",
        "",
        "  CAMPOS MAIS ALTERADOS:",
    ]

    for campo, count in list(r.get("campos_mais_alterados", {}).items())[:15]:
        lines.append(f"    {campo:<40} {count:>6,}")

    # Top 10 fundos com mais alterações
    alterados = diff_report.get("detalhes", {}).get("alterados", [])
    if alterados:
        lines.append("")
        lines.append("  TOP 10 FUNDOS COM MAIS ALTERAÇÕES:")
        for item in alterados[:10]:
            cnpj = item["cnpj_fundo"]
            razao = (item.get("razao_social") or "")[:50]
            n = item["total_campos_alterados"]
            lines.append(f"    {cnpj}  ({n} campos)  {razao}")

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


def save_diff_report(diff_report: dict, output_dir: str = ".") -> tuple:
    """Salva o relatório de diff em JSON e TXT.

    Returns:
        (json_path, txt_path)
    """
    json_path = os.path.join(output_dir, "diff_report.json")
    txt_path = os.path.join(output_dir, "diff_summary.txt")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(diff_report, f, indent=2, ensure_ascii=False)

    summary = generate_summary_text(diff_report)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(summary)

    return json_path, txt_path
