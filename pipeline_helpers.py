"""
Pipeline Helpers — SQL transacional, métricas e integração com validação/diff.
Importado pelo main.py para substituir a geração SQL linear.
"""
import json
import logging
import os
import time
from datetime import datetime

import data_validator as dv
import diff_engine as de

logger = logging.getLogger(__name__)

DEFAULT_PROCEDURES = {
    "razaoSocial": "CETIP.P_ATUALIZA_RAZAO_SOCIAL",
    "administrador": "CETIP.P_ATUALIZA_ADMINISTRADOR",
    "gestor": "CETIP.P_ATUALIZA_GESTOR",
    "escriturador": "CETIP.P_ATUALIZA_ESCRITURADOR",
    "custodiante": "CETIP.P_ATUALIZA_CUSTODIANTE",
    "naturezaEconomica": "CETIP.P_ATUALIZA_NATUREZA_ECONOMICA",
    "naturezaJuridica": "CETIP.P_ATUALIZA_NATUREZA_JURIDICA",
    "diretorResponsavel": "CETIP.P_ATUALIZA_DIRETOR_RESPONSAVEL",
    "dataConstituicao": "CETIP.P_ATUALIZA_DATA_CONSTITUICAO",
    "dataRegistro": "CETIP.P_ATUALIZA_DATA_REGISTRO",
    "dataSituacao": "CETIP.P_ATUALIZA_DATA_SITUACAO",
    "codigoCVM": "CETIP.P_ATUALIZA_CODIGO_CVM",
    "administradorEndereco": "CETIP.P_ATUALIZA_ADM_ENDERECO",
    "administradorTelefones": "CETIP.P_ATUALIZA_ADM_TELEFONES",
    "administradorEmail": "CETIP.P_ATUALIZA_ADM_EMAIL",
    "gestorEndereco": "CETIP.P_ATUALIZA_GESTOR_ENDERECO",
    "gestorTelefones": "CETIP.P_ATUALIZA_GESTOR_TELEFONES",
    "gestorEmail": "CETIP.P_ATUALIZA_GESTOR_EMAIL",
    "escrituradorEndereco": "CETIP.P_ATUALIZA_ESCRITURADOR_ENDERECO",
    "escrituradorTelefones": "CETIP.P_ATUALIZA_ESCRITURADOR_TELEFONES",
    "escrituradorEmail": "CETIP.P_ATUALIZA_ESCRITURADOR_EMAIL",
    "custodianteEndereco": "CETIP.P_ATUALIZA_CUSTODIANTE_ENDERECO",
    "custodianteTelefones": "CETIP.P_ATUALIZA_CUSTODIANTE_TELEFONES",
    "custodianteEmail": "CETIP.P_ATUALIZA_CUSTODIANTE_EMAIL",
    "tipoFundo": "CETIP.P_ATUALIZA_TIPO_FUNDO",
    "denominacaoSocialFundo": "CETIP.P_ATUALIZA_DENOMINACAO_FUNDO",
    "denominacaoSocialClasse": "CETIP.P_ATUALIZA_DENOMINACAO_CLASSE",
}


def esc_sql(s):
    """Escape de aspas simples para SQL."""
    return dv.sanitize_text(str(s or "")).replace("'", "''")


def _collect_fund_procedures(r, procedures, invalid_fields=None):
    """Coleta as chamadas de procedure para um único fundo.
    Retorna lista de strings SQL (sem SAVEPOINT, só as chamadas).
    """
    if invalid_fields is None:
        invalid_fields = set()

    cnpj = r.get("cnpj_fundo")
    procs = []
    proc_counts = {}

    def _add(key, sql_str):
        if procedures.get(key):
            procs.append(sql_str)
            proc_counts[procedures[key]] = proc_counts.get(procedures[key], 0) + 1

    # Razão Social
    if r.get("razao_social_final") and "razao_social_final" not in invalid_fields:
        rz = r.get("razao_social_normalizada") or r.get("razao_social_final")
        _add("razaoSocial", f"{procedures['razaoSocial']}('{cnpj}','{esc_sql(rz)}');")

    # Administrador
    if r.get("administrador_id") and r.get("administrador_nome") and "administrador_id" not in invalid_fields:
        _add("administrador", f"{procedures['administrador']}('{cnpj}','{r['administrador_id']}','{esc_sql(r['administrador_nome'])}');")

    # Gestor — TRAVA 3: só se gestor único
    is_multi = r.get("multiplos_gestores")
    if not is_multi and r.get("gestor_id") and r.get("gestor_nome") and "gestor_id" not in invalid_fields:
        _add("gestor", f"{procedures['gestor']}('{cnpj}','{r['gestor_id']}','{esc_sql(r['gestor_nome'])}');")

    # Escriturador
    if r.get("escriturador_id") and r.get("escriturador_nome") and "escriturador_id" not in invalid_fields:
        _add("escriturador", f"{procedures['escriturador']}('{cnpj}','{r['escriturador_id']}','{esc_sql(r['escriturador_nome'])}');")

    # Custodiante
    if r.get("custodiante_id") and r.get("custodiante_nome") and "custodiante_id" not in invalid_fields:
        _add("custodiante", f"{procedures['custodiante']}('{cnpj}','{r['custodiante_id']}','{esc_sql(r['custodiante_nome'])}');")

    # Natureza Econômica
    if r.get("natureza_economica_final"):
        _add("naturezaEconomica", f"{procedures['naturezaEconomica']}('{cnpj}','{esc_sql(r['natureza_economica_final'])}');")

    # TRAVA 2: Natureza Jurídica DESATIVADA PERMANENTEMENTE

    # Diretor
    if r.get("diretor_responsavel"):
        _add("diretorResponsavel", f"{procedures['diretorResponsavel']}('{cnpj}','{esc_sql(r['diretor_responsavel'])}');")

    # Datas (só se validação passou)
    if r.get("data_constituicao") and "data_constituicao" not in invalid_fields:
        _add("dataConstituicao", f"{procedures['dataConstituicao']}('{cnpj}',TO_DATE('{r['data_constituicao']}','YYYY-MM-DD'));")
    if r.get("data_registro") and "data_registro" not in invalid_fields:
        _add("dataRegistro", f"{procedures['dataRegistro']}('{cnpj}',TO_DATE('{r['data_registro']}','YYYY-MM-DD'));")
    if r.get("data_situacao") and "data_situacao" not in invalid_fields:
        _add("dataSituacao", f"{procedures['dataSituacao']}('{cnpj}',TO_DATE('{r['data_situacao']}','YYYY-MM-DD'));")

    # Código CVM
    if r.get("codigo_cvm"):
        _add("codigoCVM", f"{procedures['codigoCVM']}('{cnpj}','{esc_sql(r['codigo_cvm'])}');")

    # Endereços e contatos — Administrador
    if r.get("administrador_endereco"):
        _add("administradorEndereco", f"{procedures['administradorEndereco']}('{cnpj}','{esc_sql(r['administrador_endereco'])}');")
    if r.get("administrador_telefones"):
        _add("administradorTelefones", f"{procedures['administradorTelefones']}('{cnpj}','{esc_sql(r['administrador_telefones'])}');")
    if r.get("administrador_email"):
        _add("administradorEmail", f"{procedures['administradorEmail']}('{cnpj}','{esc_sql(r['administrador_email'])}');")

    # Endereços e contatos — Gestor (TRAVA 3)
    if not is_multi:
        if r.get("gestor_endereco"):
            _add("gestorEndereco", f"{procedures['gestorEndereco']}('{cnpj}','{esc_sql(r['gestor_endereco'])}');")
        if r.get("gestor_telefones"):
            _add("gestorTelefones", f"{procedures['gestorTelefones']}('{cnpj}','{esc_sql(r['gestor_telefones'])}');")
        if r.get("gestor_email"):
            _add("gestorEmail", f"{procedures['gestorEmail']}('{cnpj}','{esc_sql(r['gestor_email'])}');")

    # Endereços e contatos — Escriturador
    if r.get("escriturador_endereco"):
        _add("escrituradorEndereco", f"{procedures['escrituradorEndereco']}('{cnpj}','{esc_sql(r['escriturador_endereco'])}');")
    if r.get("escriturador_telefones"):
        _add("escrituradorTelefones", f"{procedures['escrituradorTelefones']}('{cnpj}','{esc_sql(r['escriturador_telefones'])}');")
    if r.get("escriturador_email"):
        _add("escrituradorEmail", f"{procedures['escrituradorEmail']}('{cnpj}','{esc_sql(r['escriturador_email'])}');")

    # Endereços e contatos — Custodiante
    if r.get("custodiante_endereco"):
        _add("custodianteEndereco", f"{procedures['custodianteEndereco']}('{cnpj}','{esc_sql(r['custodiante_endereco'])}');")
    if r.get("custodiante_telefones"):
        _add("custodianteTelefones", f"{procedures['custodianteTelefones']}('{cnpj}','{esc_sql(r['custodiante_telefones'])}');")
    if r.get("custodiante_email"):
        _add("custodianteEmail", f"{procedures['custodianteEmail']}('{cnpj}','{esc_sql(r['custodiante_email'])}');")

    # Tipo Fundo e Denominações
    if r.get("tipo_fundo"):
        _add("tipoFundo", f"{procedures['tipoFundo']}('{cnpj}','{esc_sql(r['tipo_fundo'])}');")
    if r.get("denominacao_social_fundo"):
        _add("denominacaoSocialFundo", f"{procedures['denominacaoSocialFundo']}('{cnpj}','{esc_sql(r['denominacao_social_fundo'])}');")
    if r.get("denominacao_social_classe"):
        _add("denominacaoSocialClasse", f"{procedures['denominacaoSocialClasse']}('{cnpj}','{esc_sql(r['denominacao_social_classe'])}');")

    return procs, proc_counts, is_multi


def generate_transactional_sql(results, procedures, validations=None, error_threshold=0.05, only_cnpjs=None, label="COMPLETO"):
    """Gera SQL transacional com SAVEPOINT por CNPJ e COMMIT condicional.

    Args:
        results: lista de registros consolidados
        procedures: dict mapeando nomes para procedures Oracle
        validations: lista de validações (retorno de validate_fund)
        error_threshold: taxa máxima de erro antes de ROLLBACK total
        only_cnpjs: se fornecido, gera SQL APENAS para esses CNPJs (para delta)
        label: rótulo no cabeçalho SQL ("COMPLETO" ou "DELTA")

    Returns: (sql_text, stats_dict)
    """
    if validations is None:
        validations = [{"rejected": False, "invalid_fields": set()} for _ in results]

    _cancelados = 0
    _gestores_ambiguos = 0
    _rejeitados_validacao = 0
    _fundos_no_sql = 0
    proc_totals = {}

    # Coleta procedures por fundo
    fund_blocks = []
    for r, v in zip(results, validations):
        cnpj = r.get("cnpj_fundo")
        if not cnpj:
            continue

        # Filtro delta: se only_cnpjs definido, ignora CNPJs fora do set
        if only_cnpjs is not None and cnpj not in only_cnpjs:
            continue

        # TRAVA 1: Ignora cancelados
        sit = (r.get("situacao_final") or "").upper()
        if "CANCELAD" in sit:
            _cancelados += 1
            continue

        # Validação: rejeita fundo se CNPJ inválido
        if v.get("rejected"):
            _rejeitados_validacao += 1
            continue

        inv_fields = v.get("invalid_fields", set())
        procs, counts, is_multi = _collect_fund_procedures(r, procedures, inv_fields)
        if is_multi:
            _gestores_ambiguos += 1

        if procs:
            fund_blocks.append((cnpj, procs))
            _fundos_no_sql += 1
            for k, c in counts.items():
                proc_totals[k] = proc_totals.get(k, 0) + c

    # Monta SQL transacional
    threshold_pct = int(error_threshold * 100)
    lines = []
    lines.append(f"-- =====================================================")
    lines.append(f"-- SQL TRANSACIONAL GERADO AUTOMATICAMENTE [{label}]")
    lines.append(f"-- Pipeline B3 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"-- Fundos: {_fundos_no_sql} | Threshold erro: {threshold_pct}%")
    lines.append(f"-- =====================================================")
    lines.append("SET SERVEROUTPUT ON;")
    lines.append("DECLARE")
    lines.append("  v_erros  NUMBER := 0;")
    lines.append("  v_total  NUMBER := 0;")
    lines.append("BEGIN")

    for cnpj, procs in fund_blocks:
        lines.append(f"  -- FUNDO: {cnpj}")
        lines.append(f"  SAVEPOINT sp_{cnpj};")
        lines.append(f"  BEGIN")
        for p in procs:
            lines.append(f"    {p}")
        lines.append(f"    v_total := v_total + 1;")
        lines.append(f"  EXCEPTION")
        lines.append(f"    WHEN OTHERS THEN")
        lines.append(f"      ROLLBACK TO sp_{cnpj};")
        lines.append(f"      v_erros := v_erros + 1;")
        lines.append(f"      DBMS_OUTPUT.PUT_LINE('ERRO [' || '{cnpj}' || ']: ' || SQLERRM);")
        lines.append(f"  END;")

    lines.append(f"  -- COMMIT CONDICIONAL (threshold: {threshold_pct}%)")
    lines.append(f"  IF v_erros = 0 OR (v_erros < v_total * {error_threshold}) THEN")
    lines.append(f"    COMMIT;")
    lines.append(f"    DBMS_OUTPUT.PUT_LINE('COMMIT OK: ' || v_total || ' fundos | ' || v_erros || ' erros');")
    lines.append(f"  ELSE")
    lines.append(f"    ROLLBACK;")
    lines.append(f"    DBMS_OUTPUT.PUT_LINE('ROLLBACK TOTAL: taxa de erro excede {threshold_pct}%: ' || v_erros || '/' || v_total);")
    lines.append(f"    RAISE_APPLICATION_ERROR(-20001, 'Taxa de erro: ' || v_erros || '/' || v_total);")
    lines.append(f"  END IF;")
    lines.append("END;")
    lines.append("/")

    sql_text = "\n".join(lines)
    total_proc_lines = sum(len(b[1]) for b in fund_blocks)

    stats = {
        "fundos_no_sql": _fundos_no_sql,
        "cancelados_ignorados": _cancelados,
        "gestores_ambiguos": _gestores_ambiguos,
        "rejeitados_validacao": _rejeitados_validacao,
        "total_linhas_sql": len(lines),
        "total_procedures": total_proc_lines,
        "procedures_por_tipo": proc_totals,
        "error_threshold": error_threshold,
    }

    return sql_text, stats


def generate_delta_sql(results, procedures, validations, diff_report, error_threshold=0.05):
    """Gera SQL contendo APENAS os fundos novos ou alterados (delta).

    Args:
        results: lista completa de registros
        procedures: dict de procedures Oracle
        validations: lista de validações
        diff_report: relatório de diff gerado pelo diff_engine
        error_threshold: threshold de erro

    Returns: (sql_text, delta_stats) ou (None, None) se não houver diff
    """
    if not diff_report:
        return None, None

    changed_cnpjs = de.get_changed_cnpjs(diff_report)
    if not changed_cnpjs:
        logger.info("Delta SQL: nenhum fundo novo ou alterado — nada a gerar.")
        return None, None

    resumo = diff_report.get("resumo", {})
    logger.info(
        f"Gerando SQL DELTA para {len(changed_cnpjs)} fundos "
        f"({resumo.get('fundos_novos', 0)} novos + {resumo.get('fundos_alterados', 0)} alterados)"
    )

    sql_text, stats = generate_transactional_sql(
        results, procedures, validations, error_threshold,
        only_cnpjs=changed_cnpjs, label="DELTA"
    )

    stats["delta_cnpjs_novos"] = resumo.get("fundos_novos", 0)
    stats["delta_cnpjs_alterados"] = resumo.get("fundos_alterados", 0)

    return sql_text, stats


def run_validation(results):
    """Executa validação em todos os resultados. Retorna (validations, quality_report)."""
    validator = dv.DataValidator() if hasattr(dv, 'DataValidator') else None
    validations = []
    for r in results:
        validations.append(dv.validate_fund(r))
    return validations


def run_diff(output_path, results):
    """Executa diff com a execução anterior, se existir. Retorna diff_report ou None."""
    prev_records = de.load_previous_records(output_path)
    if prev_records is None:
        logger.info("Nenhuma execução anterior encontrada para diff.")
        return None

    logger.info(f"Comparando com execução anterior ({len(prev_records)} registros)...")
    diff_report = de.compute_diff(prev_records, results)

    r = diff_report.get("resumo", {})
    logger.info(
        f"Diff: {r.get('fundos_novos', 0)} novos | "
        f"{r.get('fundos_alterados', 0)} alterados | "
        f"{r.get('fundos_inalterados', 0)} inalterados"
    )

    output_dir = os.path.dirname(output_path) or "."
    de.save_diff_report(diff_report, output_dir)
    return diff_report


def build_execution_metrics(timings, sql_stats, quality_report, diff_report, results):
    """Monta o JSON de métricas da execução."""
    exec_id = f"exec_{datetime.now().strftime('%Y-%m-%d_%H-%M')}"

    metrics = {
        "execution_id": exec_id,
        "started_at": timings.get("pipeline_start", ""),
        "finished_at": datetime.now().isoformat(),
        "duration_total_seconds": round(timings.get("total_seconds", 0), 1),
        "etapas": {},
        "eficiencia": {
            "total_fundos_cvm": len(results),
            "fundos_no_sql": sql_stats.get("fundos_no_sql", 0),
            "cancelados_ignorados": sql_stats.get("cancelados_ignorados", 0),
            "gestores_ambiguos": sql_stats.get("gestores_ambiguos", 0),
            "rejeitados_validacao": sql_stats.get("rejeitados_validacao", 0),
            "total_procedures": sql_stats.get("total_procedures", 0),
            "total_linhas_sql": sql_stats.get("total_linhas_sql", 0),
            "procedures_por_tipo": sql_stats.get("procedures_por_tipo", {}),
        },
    }

    # Adiciona timings das etapas
    for step_name in ("download_cvm", "parse_csvs", "anbima", "download_adm", "consolidacao", "validacao", "diff", "geracao_json", "geracao_sql"):
        if step_name in timings:
            metrics["etapas"][step_name] = timings[step_name]

    if quality_report:
        metrics["data_quality"] = quality_report

    if diff_report:
        metrics["delta"] = diff_report.get("resumo", {})

    return metrics


def save_metrics(metrics, output_dir="."):
    """Salva métricas em JSON e append no log histórico."""
    metrics_path = os.path.join(output_dir, "execution_metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    # Append ao log histórico
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "executions.jsonl")
    summary = {
        "execution_id": metrics.get("execution_id"),
        "timestamp": metrics.get("finished_at"),
        "duration_s": metrics.get("duration_total_seconds"),
        "fundos": metrics.get("eficiencia", {}).get("total_fundos_cvm"),
        "sql_lines": metrics.get("eficiencia", {}).get("total_linhas_sql"),
        "cancelados": metrics.get("eficiencia", {}).get("cancelados_ignorados"),
    }
    delta = metrics.get("delta")
    if delta:
        summary["novos"] = delta.get("fundos_novos", 0)
        summary["alterados"] = delta.get("fundos_alterados", 0)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(summary, ensure_ascii=False) + "\n")

    logger.info(f"Métricas salvas em {metrics_path}")
    return metrics_path
