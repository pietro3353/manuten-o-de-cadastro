import argparse
import asyncio
import csv
import io
import json
import logging
import os
import time
import zipfile
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

# Configurações de Log
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CVM_ZIP_URL = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip"
ADM_CART_ZIP_URL = "https://dados.cvm.gov.br/dados/ADM_CART/CAD/DADOS/cad_adm_cart.zip"
ANBIMA_OAUTH_URL = "https://api.anbima.com.br/oauth/access-token"
ANBIMA_LOTE_URL = "https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/dados-cadastrais/lote"

DEFAULT_CONCURRENCY = 50
MAX_RETRIES = 3

# Estado Global do Servidor
global_job_status = {"status": "idle", "message": "Aguardando", "progress": 0, "total": 0}

def normalize_text(value: str = "") -> str:
    import unicodedata
    txt = unicodedata.normalize("NFD", str(value or ""))
    txt = "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")
    return " ".join("".join(ch if ch.isalnum() or ch.isspace() else " " for ch in txt).split())

def find_col(header: List[str], candidates: List[str]) -> int:
    for candidate in candidates:
        for i, h in enumerate(header):
            if h.strip().upper() == candidate.upper():
                return i
    return -1

B3_NATUREZAS = [
    "FUNCINE FUNDO DE FINANCIAMENTO DA INDUSTRIA CINEMATOGRAFICA NACIONAL",
    "FUNDO DE APOSENTADORIA PROGRAMADA INDIVIDUAL - FAPI",
    "FUNDO DE CONVERSAO",
    "FUNDO DE INVESTIMENTO CAMBIAL",
    "FUNDO DE INVESTIMENTO CAMBIAL DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO CULTURAL E ARTISTICO - FICART",
    "FUNDO DE INVESTIMENTO DE CURTO PRAZO",
    "FUNDO DE INVESTIMENTO DO FUNDO DE GARANTIA DO TEMPO DE SERVICO",
    "FUNDO DE INVESTIMENTO EM ACOES",
    "FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE PARTICIPACOES - FICFIP",
    "FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS",
    "FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA",
    "FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO EM INDICE DE MERCADO",
    "FUNDO DE INVESTIMENTO EM PARTICIPACOES",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FIDC - FIQFIDC",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO CAMBIAL",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO CAMBIAL DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO DE CURTO PRAZO",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO DE RENDA FIXA",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO DE RENDA FIXA DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO EM ACOES",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO MULTIMERCADO",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO MULTIMERCADO DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO REFERENCIADOS",
    "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO REFERENCIADOS DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO EM RENDA FIXA",
    "FUNDO DE INVESTIMENTO EM RENDA FIXA DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO IMOBILIARIO",
    "FUNDO DE INVESTIMENTO MULTIMERCADO",
    "FUNDO DE INVESTIMENTO MULTIMERCADO DE LONGO PRAZO",
    "FUNDO DE INVESTIMENTO NAS CADEIAS PRODUTIVAS AGROINDUSTRIAIS",
    "FUNDO DE INVESTIMENTO PREVIDENCIARIO",
    "FUNDO DE INVESTIMENTO REFERENCIADO",
    "FUNDO DE INVESTIMENTO REFERENCIADO DE LONGO PRAZO",
    "FUNDO DE PRIVATIZACAO - CP",
    "FUNDO DE PRIVATIZACAO - DIVIDA SECURITIZADA",
    "FUNDO DE PRIVATIZACAO - FGTS",
    "FUNDO GARANTIDOR DE OPERACOES",
    "FUNDO MUTUO DE INVESTIMENTO EM EMPRESAS EMERGENTES"
]

def map_natureza_economica_b3(tipo_fundo: str, tipo_classe: str, class_anbima: str, nome_fundo: str) -> str:
    import unicodedata
    def clean(text):
        if not text: return ""
        txt = unicodedata.normalize("NFD", str(text).upper())
        return "".join(ch for ch in txt if unicodedata.category(ch) != "Mn").strip()
    
    tf = clean(tipo_fundo)
    tc = clean(tipo_classe)
    ca = clean(class_anbima)
    nf = clean(nome_fundo)

    for nat in B3_NATUREZAS:
        nat_clean = clean(nat)
        if nat_clean == tf or nat_clean == tc:
            return nat

    tf_words = tf.split()
    nf_words = nf.split()
    tc_words = tc.split()

    # Modificadores Globais
    is_fic = "FIC" in nf_words or "COTAS" in nf or "QUOTAS" in nf or "FICFIDC" in nf or "FIQFIDC" in nf or "FICFIP" in nf
    is_lp = "LONGO PRAZO" in tc or "LONGO PRAZO" in nf or " LP " in nf or nf.endswith(" LP")
    is_cp = "CURTO PRAZO" in tc or "CURTO PRAZO" in nf or " CP " in nf or nf.endswith(" CP")

    # ==========================================
    # PASSO 1: Tipos Macro (Categorias Exclusivas)
    # ==========================================
    
    # Macro FII
    if "FII" in tf_words or "IMOBILIARIO" in tf or "FII" in nf_words or "IMOBILIARIO" in nf:
        return "FUNDO DE INVESTIMENTO IMOBILIARIO"

    # Macro FIDC
    if "FIDC" in tf_words or "DIREITOS CREDITORIOS" in tf or "FIDC" in nf_words or "DIREITOS CREDITORIOS" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FIDC - FIQFIDC"
        return "FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS"

    # Macro FIP
    if "FIP" in tf_words or "PARTICIPACOES" in tf or "FIP" in nf_words or "PARTICIPACOES" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE PARTICIPACOES - FICFIP"
        return "FUNDO DE INVESTIMENTO EM PARTICIPACOES"

    # ==========================================
    # PASSO 2: A Árvore dos FIs (Baseado em tipo_classe e nome_fundo)
    # ==========================================
    
    # Ações
    if "ACOES" in tc_words or "ACOES" in nf or "ACAO" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO EM ACOES"
        return "FUNDO DE INVESTIMENTO EM ACOES"

    # Multimercado
    if "MULTIMERCADO" in tc_words or "MULTIMERCADO" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO MULTIMERCADO DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO MULTIMERCADO"
        return "FUNDO DE INVESTIMENTO MULTIMERCADO DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO MULTIMERCADO"

    # Renda Fixa
    if "RENDA FIXA" in tc or "RENDA FIXA" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO DE RENDA FIXA DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO DE RENDA FIXA"
        return "FUNDO DE INVESTIMENTO EM RENDA FIXA DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO EM RENDA FIXA"

    # Cambial
    if "CAMBIAL" in tc or "CAMBIAL" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO CAMBIAL DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO CAMBIAL"
        return "FUNDO DE INVESTIMENTO CAMBIAL DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO CAMBIAL"

    # Referenciado
    if "REFERENCIADO" in tc or "REFERENCIADO" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO REFERENCIADOS DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO REFERENCIADOS"
        return "FUNDO DE INVESTIMENTO REFERENCIADO DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO REFERENCIADO"

    # Dívida Externa
    if "DIVIDA EXTERNA" in tc or "DIVIDA EXTERNA" in nf:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA"
        return "FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA DE LONGO PRAZO" if is_lp else "FUNDO DE INVESTIMENTO EM DIVIDA EXTERNA"

    # Previdenciário
    if "PREVIDENCIARIO" in tc or "PREVIDENCIARIO" in nf:
        return "FUNDO DE INVESTIMENTO PREVIDENCIARIO"

    # Índice de Mercado
    if "INDICE DE MERCADO" in tc or "INDICE DE MERCADO" in nf:
        return "FUNDO DE INVESTIMENTO EM INDICE DE MERCADO"

    # Curto Prazo
    if is_cp:
        if is_fic:
            return "FUNDO DE INVESTIMENTO EM QUOTAS DE FUNDO DE INVESTIMENTO DE CURTO PRAZO"
        return "FUNDO DE INVESTIMENTO DE CURTO PRAZO"

    return tipo_classe if tipo_classe else "Outros"

async def download_cvm_zip(client: httpx.AsyncClient) -> bytes:
    logger.info(f"Fazendo download do arquivo ZIP da CVM: {CVM_ZIP_URL}")
    response = await client.get(CVM_ZIP_URL, timeout=120.0)
    response.raise_for_status()
    logger.info("Download do ZIP concluído.")
    return response.content

def parse_cvm_csvs(zip_bytes: bytes) -> Dict[str, Dict[str, Any]]:
    logger.info("Lendo arquivos do ZIP em memória (encoding cp1252)...")
    funds = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        fundo_filename = next((n for n in z.namelist() if "registro_fundo.csv" in n.lower()), None)
        classe_filename = next((n for n in z.namelist() if "registro_classe.csv" in n.lower()), None)

        if not fundo_filename:
            raise ValueError("registro_fundo.csv não encontrado no ZIP.")

        logger.info("Processando registro_fundo.csv...")
        with z.open(fundo_filename) as f:
            content = f.read()
            if content.startswith(b'\xef\xbb\xbf'):
                content = content[3:]
            text = content.decode("cp1252", errors="replace")
            reader = csv.reader(io.StringIO(text), delimiter=";")
            header = next(reader)
            
            idx_id = find_col(header, ['ID_Registro_Fundo'])
            idx_cnpj = find_col(header, ['CNPJ_Fundo'])
            idx_cvm = find_col(header, ['Codigo_CVM'])
            idx_razao = find_col(header, ['Denominacao_Social'])
            idx_sit = find_col(header, ['Situacao'])
            idx_cnpj_adm = find_col(header, ['CNPJ_Administrador'])
            idx_adm = find_col(header, ['Administrador'])
            idx_cnpj_gestor = find_col(header, ['CPF_CNPJ_Gestor'])
            idx_gestor = find_col(header, ['Gestor'])
            
            # Novos campos CVM
            idx_diretor = find_col(header, ['Diretor'])
            idx_dt_const = find_col(header, ['Data_Constituicao'])
            idx_dt_reg = find_col(header, ['Data_Registro'])
            idx_dt_sit = find_col(header, ['Data_Inicio_Situacao'])
            idx_tipo_fundo = find_col(header, ['Tipo_Fundo'])

            # Helper para padronizar CNPJs (inclusive recolocar zeros à esquerda perdidos pelo CSV)
            def clean_cnpj(c):
                num = "".join(x for x in c if x.isdigit())
                return num.zfill(14) if num else ""

            for row in reader:
                if len(row) <= max(idx_id, idx_cnpj): continue
                cnpj = clean_cnpj(row[idx_cnpj])
                id_reg = row[idx_id].strip()
                if not cnpj or not id_reg: continue
                
                g_cnpj = clean_cnpj(row[idx_cnpj_gestor]) if idx_cnpj_gestor >= 0 else ""
                g_nome = normalize_text(row[idx_gestor].strip()) if idx_gestor >= 0 else ""
                
                if id_reg in funds:
                    if g_cnpj or g_nome:
                        gestor_existe = any(g["cnpj"] == g_cnpj and g["nome"] == g_nome for g in funds[id_reg].get("gestores_cvm", []))
                        if not gestor_existe:
                            funds[id_reg].setdefault("gestores_cvm", []).append({"cnpj": g_cnpj, "nome": g_nome})
                    continue
                
                funds[id_reg] = {
                    "id_registro_fundo": id_reg,
                    "cnpj_fundo": cnpj,
                    "codigo_cvm_fundo": row[idx_cvm].strip().zfill(7) if idx_cvm >= 0 and row[idx_cvm].strip() else "",
                    "razao_social_cvm": row[idx_razao].strip() if idx_razao >= 0 else "",
                    "razao_social_normalizada": normalize_text(row[idx_razao].strip()) if idx_razao >= 0 else "",
                    "situacao_cvm": row[idx_sit].strip() if idx_sit >= 0 else "",
                    "administrador_cnpj_csv": clean_cnpj(row[idx_cnpj_adm]) if idx_cnpj_adm >= 0 else "",
                    "administrador_nome_csv": normalize_text(row[idx_adm].strip()) if idx_adm >= 0 else "",
                    "gestor_cnpj_csv": g_cnpj,
                    "gestor_nome_csv": g_nome,
                    "gestores_cvm": [{"cnpj": g_cnpj, "nome": g_nome}] if (g_cnpj or g_nome) else [],
                    
                    "diretor_responsavel_cvm": row[idx_diretor].strip() if idx_diretor >= 0 else "",
                    "data_constituicao_cvm": row[idx_dt_const].strip() if idx_dt_const >= 0 else "",
                    "data_registro_cvm": row[idx_dt_reg].strip() if idx_dt_reg >= 0 else "",
                    "data_situacao_cvm": row[idx_dt_sit].strip() if idx_dt_sit >= 0 else "",
                    "tipo_fundo_cvm": row[idx_tipo_fundo].strip() if idx_tipo_fundo >= 0 else "",
                    "denominacao_social_fundo_cvm": row[idx_razao].strip() if idx_razao >= 0 else "",
                    
                    "cnpj_classe_csv": "",
                    "codigo_cvm_classe": "",
                    "razao_social_classe_csv": "",
                    "forma_condominio_csv": "",
                    "natureza_economica_cvm": "",
                    "natureza_juridica_cvm": "",
                    "tipo_classe_cvm": "",
                    "custodiante_cnpj_csv": "",
                    "custodiante_nome_csv": "",
                    "controlador_cnpj_csv": "",
                    "controlador_nome_csv": ""
                }

        if classe_filename:
            logger.info("Processando registro_classe.csv...")
            with z.open(classe_filename) as f:
                content = f.read()
                if content.startswith(b'\xef\xbb\xbf'):
                    content = content[3:]
                text = content.decode("cp1252", errors="replace")
                reader = csv.reader(io.StringIO(text), delimiter=";")
                header = next(reader)
                
                idx_id = find_col(header, ['ID_Registro_Fundo'])
                idx_cnpj_cls = find_col(header, ['CNPJ_Classe'])
                idx_cvm_cls = find_col(header, ['Codigo_CVM'])
                idx_razao = find_col(header, ['Denominacao_Social'])
                idx_forma = find_col(header, ['Forma_Condominio'])
                idx_tipo_classe = find_col(header, ['Tipo_Classe'])
                idx_class_anb = find_col(header, ['Classificacao_Anbima'])
                idx_cnpj_cust = find_col(header, ['CNPJ_Custodiante'])
                idx_cust = find_col(header, ['Custodiante'])
                idx_cnpj_ctrl = find_col(header, ['CNPJ_Controlador'])
                idx_ctrl = find_col(header, ['Controlador'])

                for row in reader:
                    if len(row) <= idx_id: continue
                    id_reg = row[idx_id].strip()
                    if id_reg in funds:
                        fund = funds[id_reg]
                        if idx_cnpj_cls >= 0 and row[idx_cnpj_cls].strip(): fund["cnpj_classe_csv"] = clean_cnpj(row[idx_cnpj_cls])
                        if idx_cvm_cls >= 0 and row[idx_cvm_cls].strip(): fund["codigo_cvm_classe"] = row[idx_cvm_cls].strip().zfill(7)
                        if idx_razao >= 0 and row[idx_razao].strip(): 
                            fund["razao_social_classe_csv"] = row[idx_razao].strip()
                            fund["denominacao_social_classe_csv"] = row[idx_razao].strip()
                        
                        forma = row[idx_forma].strip() if idx_forma >= 0 else ""
                        fund["forma_condominio_csv"] = forma
                        fund["natureza_juridica_cvm"] = forma
                        
                        tipo_cls = row[idx_tipo_classe].strip() if idx_tipo_classe >= 0 else ""
                        anbima_cls = row[idx_class_anb].strip() if idx_class_anb >= 0 else ""
                        fund["natureza_economica_cvm"] = map_natureza_economica_b3(fund.get("tipo_fundo_cvm", ""), tipo_cls, anbima_cls, fund.get("razao_social_normalizada", ""))
                        fund["tipo_classe_cvm"] = tipo_cls
                        
                        if idx_cnpj_cust >= 0 and row[idx_cnpj_cust].strip(): fund["custodiante_cnpj_csv"] = clean_cnpj(row[idx_cnpj_cust])
                        if idx_cust >= 0 and row[idx_cust].strip(): fund["custodiante_nome_csv"] = normalize_text(row[idx_cust].strip())
                        
                        if idx_cnpj_ctrl >= 0 and row[idx_cnpj_ctrl].strip(): fund["controlador_cnpj_csv"] = clean_cnpj(row[idx_cnpj_ctrl])
                        if idx_ctrl >= 0 and row[idx_ctrl].strip(): fund["controlador_nome_csv"] = normalize_text(row[idx_ctrl].strip())

    logger.info(f"Total de fundos processados do CVM ZIP: {len(funds)}")
    return funds

async def download_and_parse_adm_cart(client: httpx.AsyncClient) -> Dict[str, Dict[str, str]]:
    logger.info(f"Fazendo download dos participantes da CVM: {ADM_CART_ZIP_URL}")
    response = await client.get(ADM_CART_ZIP_URL, timeout=120.0)
    response.raise_for_status()
    logger.info("Lendo participantes (cad_adm_cart_pj.csv) em memória...")
    
    adm_data = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        filename = next((n for n in z.namelist() if "cad_adm_cart_pj.csv" in n.lower()), None)
        if not filename:
            logger.warning("cad_adm_cart_pj.csv não encontrado no ZIP.")
            return adm_data
            
        with z.open(filename) as f:
            content = f.read()
            if content.startswith(b'\xef\xbb\xbf'):
                content = content[3:]
            text = content.decode("cp1252", errors="replace")
            reader = csv.reader(io.StringIO(text), delimiter=";")
            header = next(reader)
            
            idx_cnpj = find_col(header, ['CNPJ'])
            idx_log = find_col(header, ['LOGRADOURO'])
            idx_compl = find_col(header, ['COMPL'])
            idx_bairro = find_col(header, ['BAIRRO'])
            idx_mun = find_col(header, ['MUN'])
            idx_uf = find_col(header, ['UF'])
            idx_cep = find_col(header, ['CEP'])
            idx_ddd = find_col(header, ['DDD'])
            idx_tel = find_col(header, ['TEL'])
            idx_email = find_col(header, ['EMAIL'])
            
            for row in reader:
                if len(row) <= idx_cnpj: continue
                cnpj_raw = row[idx_cnpj].strip()
                cnpj_clean = "".join(c for c in cnpj_raw if c.isdigit()).zfill(14)
                if not cnpj_clean: continue
                
                parts_end = []
                if idx_log >= 0 and row[idx_log].strip(): parts_end.append(row[idx_log].strip())
                if idx_compl >= 0 and row[idx_compl].strip(): parts_end.append(row[idx_compl].strip())
                if idx_bairro >= 0 and row[idx_bairro].strip(): parts_end.append(row[idx_bairro].strip())
                if idx_mun >= 0 and row[idx_mun].strip(): parts_end.append(row[idx_mun].strip())
                if idx_uf >= 0 and row[idx_uf].strip(): parts_end.append(f"- {row[idx_uf].strip()}")
                
                endereco = ", ".join(p for p in parts_end if p).replace(" ,", ",")
                cep = row[idx_cep].strip() if idx_cep >= 0 else ""
                if cep:
                    endereco += f", CEP: {cep}"
                
                telefone = ""
                if idx_ddd >= 0 and idx_tel >= 0:
                    ddd = row[idx_ddd].strip()
                    tel = row[idx_tel].strip()
                    if ddd and tel:
                        telefone = f"({ddd}) {tel}"
                    elif tel:
                        telefone = tel
                        
                email = row[idx_email].strip() if idx_email >= 0 else ""
                
                adm_data[cnpj_clean] = {
                    "endereco": endereco,
                    "telefones": telefone,
                    "email": email
                }
    
    logger.info(f"Total de participantes processados: {len(adm_data)}")
    return adm_data

async def get_anbima_token(client: httpx.AsyncClient, client_id: str, client_secret: str) -> str:
    import base64
    logger.info("Obtendo token da ANBIMA...")
    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = await client.post(
        ANBIMA_OAUTH_URL,
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
        json={"grant_type": "client_credentials"},
        timeout=30.0
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("access_token", "")

async def fetch_anbima_lote(client: httpx.AsyncClient, client_id: str, token: str) -> List[Dict[str, Any]]:
    logger.info("Buscando lote de dados cadastrais da ANBIMA...")
    resp = await client.get(
        ANBIMA_LOTE_URL,
        headers={"client_id": client_id, "access_token": token, "Content-Type": "application/json"},
        timeout=120.0
    )
    resp.raise_for_status()
    data = resp.json()
    
    rows = []
    if isinstance(data, list): rows = data
    elif isinstance(data, dict):
        for key in ["content", "items", "results", "data", "fundos"]:
            if isinstance(data.get(key), list):
                rows = data[key]
                break
        if not rows and "page" in data and isinstance(data["page"], dict) and isinstance(data["page"].get("content"), list):
            rows = data["page"]["content"]
            
    mapped = []
    for r in rows:
        cnpj = r.get("identificador_fundo") or r.get("cnpj_fundo")
        if not cnpj: continue
        
        prest_adm = None
        prest_gestores = []
        for p in r.get("prestadores_fundo", []) or r.get("prestadores", []) or []:
            tipo = str(p.get("codigo_tipo_prestador") or p.get("tipo_prestador") or p.get("tipoPrestador") or "").upper()
            item = {
                "identificador": p.get("identificador") or p.get("cpf_cnpj") or p.get("cpfCnpj"),
                "nome": p.get("razao_social") or p.get("razaoSocial") or p.get("nome_ou_razao_social")
            }
            if "ADMIN" in tipo and not prest_adm: 
                prest_adm = item
            elif "GESTOR" in tipo: 
                prest_gestores.append(item)
            
        mapped.append({
            "cnpj_fundo": str(cnpj),
            "razao_social_anbima": r.get("razao_social_fundo"),
            "administrador_anbima": prest_adm,
            "gestores_anbima": prest_gestores,
            "gestor_anbima": prest_gestores[0] if prest_gestores else None
        })
        
    logger.info(f"Total de fundos processados da ANBIMA: {len(mapped)}")
    return mapped

def consolidate_fund(f: Dict[str, Any], adm_data: Dict[str, Dict[str, str]] = None) -> Dict[str, Any]:
    if adm_data is None: adm_data = {}
    def choose(*args):
        for a in args:
            if a is not None and a != "": return a
        return None

    adm_anb = f.get("_anbima", {}).get("administrador_anbima", {}) or {}
    gestor_anb = f.get("_anbima", {}).get("gestor_anbima", {}) or {}
    
    gestores_anb = f.get("_anbima", {}).get("gestores_anbima", [])
    gestores_cvm = f.get("gestores_cvm", [])

    razao_final = choose(f.get("razao_social_cvm"), f.get("_anbima", {}).get("razao_social_anbima"), f.get("razao_social_classe_csv"))
    
    adm_nome = choose(f.get("administrador_nome_csv"), adm_anb.get("nome"))
    adm_id = choose(f.get("administrador_cnpj_csv"), adm_anb.get("identificador"))
    if adm_id: adm_id = adm_id.zfill(14)
    
    gestor_nome = choose(f.get("gestor_nome_csv"), gestor_anb.get("nome"))
    gestor_id = choose(f.get("gestor_cnpj_csv"), gestor_anb.get("identificador"))
    if gestor_id: gestor_id = gestor_id.zfill(14)
    
    gestores_mapeados = []
    _vistos = set()
    
    def append_gestor(g_id, g_nome):
        g_id = str(g_id).zfill(14) if g_id else ""
        chave = g_id if g_id else g_nome
        if chave and chave not in _vistos:
            _vistos.add(chave)
            g_info = adm_data.get(g_id, {}) if g_id else {}
            gestores_mapeados.append({
                "id": g_id,
                "nome": g_nome,
                "endereco": g_info.get("endereco"),
                "telefones": g_info.get("telefones"),
                "email": g_info.get("email")
            })

    for g in gestores_cvm: append_gestor(g.get("cnpj"), g.get("nome"))
    for g in gestores_anb: append_gestor(g.get("identificador"), g.get("nome"))
    
    # Lógica heurística não destrutiva para o "Gestor Mais Provável"
    gestor_provavel = None
    if gestores_mapeados:
        if len(gestores_mapeados) == 1:
            gestor_provavel = gestores_mapeados[0]
        else:
            # 1. Tenta por raiz do CNPJ (igual ao Administrador)
            raiz_adm = adm_id[:8] if adm_id and len(adm_id) >= 8 else None
            for gm in gestores_mapeados:
                if raiz_adm and gm["id"] and gm["id"][:8] == raiz_adm:
                    gestor_provavel = gm
                    break
            # 2. Fallback: Primeiro da lista (Geralmente ANBIMA ou primeiro CVM)
            if not gestor_provavel:
                gestor_provavel = gestores_mapeados[0]
                
    # Controlador é frequentemente o Escriturador
    escriturador_nome = choose(f.get("controlador_nome_csv"), f.get("custodiante_nome_csv"))
    escriturador_id = choose(f.get("controlador_cnpj_csv"), f.get("custodiante_cnpj_csv"))
    if escriturador_id: escriturador_id = escriturador_id.zfill(14)
    
    custodiante_nome = f.get("custodiante_nome_csv")
    custodiante_id = f.get("custodiante_cnpj_csv")
    if custodiante_id: custodiante_id = custodiante_id.zfill(14)
    
    nat_eco = f.get("natureza_economica_cvm")
    nat_jur_raw = f.get("natureza_juridica_cvm", "").strip().upper()
    
    if "ABERTO" in nat_jur_raw:
        nat_jur = "FUNDO ABERTO"
    elif "FECHADO" in nat_jur_raw:
        nat_jur = "FUNDO FECHADO"
    else:
        nat_jur = nat_jur_raw

    gaps = []
    if not razao_final: gaps.append("razao_social")
    if not adm_nome and not adm_id: gaps.append("administrador")
    if not gestor_nome and not gestor_id: gaps.append("gestor")
    if not escriturador_nome and not escriturador_id: gaps.append("escriturador")
    if not custodiante_nome and not custodiante_id: gaps.append("custodiante")
    if not nat_eco: gaps.append("natureza_economica")
    if not nat_jur: gaps.append("natureza_juridica")

    adm_info = adm_data.get(adm_id, {}) if adm_id else {}
    gestor_info = adm_data.get(gestor_id, {}) if gestor_id else {}
    escriturador_info = adm_data.get(escriturador_id, {}) if escriturador_id else {}
    custodiante_info = adm_data.get(custodiante_id, {}) if custodiante_id else {}

    return {
        "cnpj_fundo": f.get("cnpj_fundo"),
        "cnpj_classe": f.get("cnpj_classe_csv"),
        "tipo_classe": f.get("tipo_classe_cvm"),
        "tipo_fundo": f.get("tipo_fundo_cvm"),
        "denominacao_social_fundo": f.get("denominacao_social_fundo_cvm"),
        "denominacao_social_classe": f.get("denominacao_social_classe_csv"),
        "id_registro_fundo": f.get("id_registro_fundo"),
        "codigo_cvm_fundo": f.get("codigo_cvm_fundo"),
        "codigo_cvm_classe": f.get("codigo_cvm_classe"),
        "codigo_cvm": f.get("codigo_cvm_classe") or f.get("codigo_cvm_fundo"),
        "razao_social_final": razao_final,
        "razao_social_normalizada": f.get("razao_social_normalizada"),
        "diretor_responsavel_cvm": f.get("diretor_responsavel_cvm"),
        "diretor_responsavel": normalize_text(f.get("diretor_responsavel_cvm")),
        "diretor_nome": normalize_text(f.get("diretor_responsavel_cvm")),
        "diretor_cpf": None,
        "data_registro": f.get("data_registro_cvm"),
        "data_constituicao": f.get("data_constituicao_cvm"),
        "administrador_id": adm_id,
        "administrador_nome": adm_nome,
        "administrador_endereco": adm_info.get("endereco"),
        "administrador_telefones": adm_info.get("telefones"),
        "administrador_email": adm_info.get("email"),
        
        "gestores_mapeados": gestores_mapeados,
        "multiplos_gestores": len(gestores_mapeados) > 1,
        "gestor_mais_provavel_id": gestor_provavel.get("id") if gestor_provavel else None,
        "gestor_mais_provavel_nome": gestor_provavel.get("nome") if gestor_provavel else None,
        
        "gestor_id": gestor_id,
        "gestor_nome": gestor_nome,
        "gestor_endereco": gestor_info.get("endereco"),
        "gestor_telefones": gestor_info.get("telefones"),
        "gestor_email": gestor_info.get("email"),
        "escriturador_id": escriturador_id,
        "escriturador_nome": escriturador_nome,
        "escriturador_endereco": escriturador_info.get("endereco"),
        "escriturador_telefones": escriturador_info.get("telefones"),
        "escriturador_email": escriturador_info.get("email"),
        "custodiante_id": custodiante_id,
        "custodiante_nome": custodiante_nome,
        "custodiante_endereco": custodiante_info.get("endereco"),
        "custodiante_telefones": custodiante_info.get("telefones"),
        "custodiante_email": custodiante_info.get("email"),
        "natureza_economica_final": nat_eco,
        "natureza_juridica_final": nat_jur,
        "situacao_final": f.get("situacao_cvm"),
        "data_situacao": f.get("data_situacao_cvm"),
        "banco_liquidante_00_cnpj": None,
        "banco_liquidante_00_nome": None,
        "banco_liquidante_44_cnpj": None,
        "banco_liquidante_44_nome": None,
        "gaps": gaps,
        "erro_detalhe": None
    }

async def execute_pipeline(req: dict):
    global global_job_status
    global_job_status = {"status": "running", "message": "Iniciando download do ZIP...", "progress": 0, "total": 0}

    concurrency = req.get("concurrency", DEFAULT_CONCURRENCY)
    output_path = req.get("output_path", "resultado_cvm.json")
    limit = req.get("limit", 0)

    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency * 2)
    timeout = httpx.Timeout(20.0)

    try:
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            zip_bytes = await download_cvm_zip(client)
            global_job_status["message"] = "Processando CSVs em Memória..."
            funds = parse_cvm_csvs(zip_bytes)

            anbima_data = []
            anb_id = req.get("anbima_client_id")
            anb_secret = req.get("anbima_client_secret")

            if anb_id and anb_secret:
                global_job_status["message"] = "Buscando dados na ANBIMA..."
                try:
                    token = await get_anbima_token(client, anb_id, anb_secret)
                    if token:
                        anbima_data = await fetch_anbima_lote(client, anb_id, token)
                except Exception as e:
                    logger.error(f"Erro ANBIMA: {e}")

            global_job_status["message"] = "Baixando dados de Administradores (CVM)..."
            try:
                adm_data = await download_and_parse_adm_cart(client)
            except Exception as e:
                logger.error(f"Erro ao baixar cad_adm_cart.zip: {e}")
                adm_data = {}

            anb_by_cnpj = {a["cnpj_fundo"]: a for a in anbima_data}
            for f in funds.values():
                f["_anbima"] = anb_by_cnpj.get(f["cnpj_fundo"], {})

            to_process = list(funds.values())
            if limit > 0:
                to_process = to_process[:limit]
                
            global_job_status["total"] = len(to_process)
            global_job_status["message"] = "Consolidando dados finais instantaneamente..."

        # Sem mais chamadas a APIs instáveis da CVM, apenas cruzamento de dados em memória
        results = []
        for i, f in enumerate(to_process):
            results.append(consolidate_fund(f, adm_data))
            if i % 10000 == 0:
                global_job_status["progress"] = i

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"status": "OK", "total": len(results), "records": results}, f, indent=2, ensure_ascii=False)
            
        # Gera e salva o arquivo SQL localmente, contornando qualquer problema de permissão do Docker/n8n
        sql_lines = []
        procedures = req.get("procedures", {
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
            "denominacaoSocialClasse": "CETIP.P_ATUALIZA_DENOMINACAO_CLASSE"
        })
        
        def esc_sql(s):
            return str(s or "").replace("'", "''")

        for r in results:
            cnpj = r.get("cnpj_fundo")
            if not cnpj: continue
            
            if procedures.get("razaoSocial") and r.get("razao_social_final"):
                rz = r.get("razao_social_normalizada") or r.get("razao_social_final")
                sql_lines.append(f"{procedures['razaoSocial']}('{cnpj}','{esc_sql(rz)}');")
            if procedures.get("administrador") and r.get("administrador_id") and r.get("administrador_nome"):
                sql_lines.append(f"{procedures['administrador']}('{cnpj}','{r.get('administrador_id')}','{esc_sql(r.get('administrador_nome'))}');")
            if procedures.get("gestor") and r.get("gestor_id") and r.get("gestor_nome"):
                sql_lines.append(f"{procedures['gestor']}('{cnpj}','{r.get('gestor_id')}','{esc_sql(r.get('gestor_nome'))}');")
            if procedures.get("escriturador") and r.get("escriturador_id") and r.get("escriturador_nome"):
                sql_lines.append(f"{procedures['escriturador']}('{cnpj}','{r.get('escriturador_id')}','{esc_sql(r.get('escriturador_nome'))}');")
            if procedures.get("custodiante") and r.get("custodiante_id") and r.get("custodiante_nome"):
                sql_lines.append(f"{procedures['custodiante']}('{cnpj}','{r.get('custodiante_id')}','{esc_sql(r.get('custodiante_nome'))}');")
            if procedures.get("naturezaEconomica") and r.get("natureza_economica_final"):
                sql_lines.append(f"{procedures['naturezaEconomica']}('{cnpj}','{esc_sql(r.get('natureza_economica_final'))}');")
            if procedures.get("naturezaJuridica") and r.get("natureza_juridica_final"):
                sql_lines.append(f"{procedures['naturezaJuridica']}('{cnpj}','{esc_sql(r.get('natureza_juridica_final'))}');")
                
            # Novos campos
            if procedures.get("diretorResponsavel") and r.get("diretor_responsavel"):
                sql_lines.append(f"{procedures['diretorResponsavel']}('{cnpj}','{esc_sql(r.get('diretor_responsavel'))}');")
            if procedures.get("dataConstituicao") and r.get("data_constituicao"):
                sql_lines.append(f"{procedures['dataConstituicao']}('{cnpj}',TO_DATE('{r.get('data_constituicao')}','YYYY-MM-DD'));")
            if procedures.get("dataRegistro") and r.get("data_registro"):
                sql_lines.append(f"{procedures['dataRegistro']}('{cnpj}',TO_DATE('{r.get('data_registro')}','YYYY-MM-DD'));")
            if procedures.get("dataSituacao") and r.get("data_situacao"):
                sql_lines.append(f"{procedures['dataSituacao']}('{cnpj}',TO_DATE('{r.get('data_situacao')}','YYYY-MM-DD'));")
            if procedures.get("codigoCVM") and r.get("codigo_cvm"):
                sql_lines.append(f"{procedures['codigoCVM']}('{cnpj}','{esc_sql(r.get('codigo_cvm'))}');")
            if procedures.get("administradorEndereco") and r.get("administrador_endereco"):
                sql_lines.append(f"{procedures['administradorEndereco']}('{cnpj}','{esc_sql(r.get('administrador_endereco'))}');")
            if procedures.get("administradorTelefones") and r.get("administrador_telefones"):
                sql_lines.append(f"{procedures['administradorTelefones']}('{cnpj}','{esc_sql(r.get('administrador_telefones'))}');")
            if procedures.get("administradorEmail") and r.get("administrador_email"):
                sql_lines.append(f"{procedures['administradorEmail']}('{cnpj}','{esc_sql(r.get('administrador_email'))}');")
            if procedures.get("gestorEndereco") and r.get("gestor_endereco"):
                sql_lines.append(f"{procedures['gestorEndereco']}('{cnpj}','{esc_sql(r.get('gestor_endereco'))}');")
            if procedures.get("gestorTelefones") and r.get("gestor_telefones"):
                sql_lines.append(f"{procedures['gestorTelefones']}('{cnpj}','{esc_sql(r.get('gestor_telefones'))}');")
            if procedures.get("gestorEmail") and r.get("gestor_email"):
                sql_lines.append(f"{procedures['gestorEmail']}('{cnpj}','{esc_sql(r.get('gestor_email'))}');")
            if procedures.get("escrituradorEndereco") and r.get("escriturador_endereco"):
                sql_lines.append(f"{procedures['escrituradorEndereco']}('{cnpj}','{esc_sql(r.get('escriturador_endereco'))}');")
            if procedures.get("escrituradorTelefones") and r.get("escriturador_telefones"):
                sql_lines.append(f"{procedures['escrituradorTelefones']}('{cnpj}','{esc_sql(r.get('escriturador_telefones'))}');")
            if procedures.get("escrituradorEmail") and r.get("escriturador_email"):
                sql_lines.append(f"{procedures['escrituradorEmail']}('{cnpj}','{esc_sql(r.get('escriturador_email'))}');")
            if procedures.get("custodianteEndereco") and r.get("custodiante_endereco"):
                sql_lines.append(f"{procedures['custodianteEndereco']}('{cnpj}','{esc_sql(r.get('custodiante_endereco'))}');")
            if procedures.get("custodianteTelefones") and r.get("custodiante_telefones"):
                sql_lines.append(f"{procedures['custodianteTelefones']}('{cnpj}','{esc_sql(r.get('custodiante_telefones'))}');")
            if procedures.get("custodianteEmail") and r.get("custodiante_email"):
                sql_lines.append(f"{procedures['custodianteEmail']}('{cnpj}','{esc_sql(r.get('custodiante_email'))}');")
            if procedures.get("tipoFundo") and r.get("tipo_fundo"):
                sql_lines.append(f"{procedures['tipoFundo']}('{cnpj}','{esc_sql(r.get('tipo_fundo'))}');")
            if procedures.get("denominacaoSocialFundo") and r.get("denominacao_social_fundo"):
                sql_lines.append(f"{procedures['denominacaoSocialFundo']}('{cnpj}','{esc_sql(r.get('denominacao_social_fundo'))}');")
            if procedures.get("denominacaoSocialClasse") and r.get("denominacao_social_classe"):
                sql_lines.append(f"{procedures['denominacaoSocialClasse']}('{cnpj}','{esc_sql(r.get('denominacao_social_classe'))}');")
                
        # Usa apenas o nome do arquivo para garantir que seja salvo na pasta do script Python (Windows), ignorando caminhos do Linux
        sql_filename = os.path.basename(req.get("sql_output_path", "update_fundos.sql"))
        with open(sql_filename, "w", encoding="utf-8") as f:
            f.write("\n".join(sql_lines))
            
        global_job_status = {"status": "done", "message": "Processo concluído super rápido!", "progress": len(to_process), "total": len(to_process)}
        logger.info("Processo concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante execução: {e}")
        global_job_status = {"status": "error", "message": str(e), "progress": 0, "total": 0}

def run_async_pipeline_thread(req):
    asyncio.run(execute_pipeline(req))

class SimpleWorkerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/status":
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(global_job_status).encode('utf-8'))
        elif self.path == "/download":
            try:
                # Retorna o arquivo gerado diretamente via HTTP
                with open("resultado_cvm.json", "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "File not found"}).encode('utf-8'))
        elif self.path == "/stop":
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"message": "Servidor sendo encerrado..."}).encode('utf-8'))
            threading.Thread(target=lambda: os._exit(0)).start()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/start":
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            try:
                req = json.loads(post_data.decode('utf-8')) if post_data else {}
            except Exception:
                req = {}

            if global_job_status["status"] == "running":
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"detail": "Já existe um processo em andamento."}).encode('utf-8'))
                return

            # Start pipeline in background
            t = threading.Thread(target=run_async_pipeline_thread, args=(req,))
            t.start()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"message": "Processo iniciado em background", "status_endpoint": "/status"}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

async def cli_main():
    parser = argparse.ArgumentParser(description="Processador Offline/Server CVM/ANBIMA ultra-rápido")
    parser.add_argument("--output", default="resultado_cvm.json", help="Caminho do arquivo JSON de saída")
    parser.add_argument("--anbima-id", help="Client ID ANBIMA")
    parser.add_argument("--anbima-secret", help="Client Secret ANBIMA")
    parser.add_argument("--limit", type=int, default=0, help="Limite de fundos para processar (0 = todos)")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Limitação de concorrência HTTP")
    parser.add_argument("--server", action="store_true", help="Rodar como servidor HTTP nativo")
    
    args = parser.parse_args()

    if args.server:
        server_address = ('0.0.0.0', 8000)
        httpd = HTTPServer(server_address, SimpleWorkerHandler)
        logger.info("Iniciando modo servidor nativo na porta 8000 (0.0.0.0)...")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        httpd.server_close()
    else:
        req = {
            "output_path": args.output,
            "anbima_client_id": args.anbima_id,
            "anbima_client_secret": args.anbima_secret,
            "concurrency": args.concurrency,
            "limit": args.limit
        }
        await execute_pipeline(req)

if __name__ == "__main__":
    asyncio.run(cli_main())


