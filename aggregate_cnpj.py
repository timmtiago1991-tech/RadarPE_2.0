#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RadarPE 2.0 — job estrutural (eixo CNPJ/CNAE) via BigQuery / Base dos Dados.

Para cada setor-piloto, agrega a base pública de CNPJ (RFB, tratada pela Base dos
Dados) por classe CNAE e produz o bloco `estrutura` do data.json:
  ativas, aberturas12m, baixas12m, capitalMedio, distribuição de porte e
  quebra por classe CNAE (ativas / aberturas / baixas).

Faz MERGE no data.json existente — preserva `news`, `feed`, `fonteSetorial`,
`teses` (que vêm do backfill de notícias e da config estática). Rode depois
(ou antes) do backfill_news.py; a ordem não importa.

Fonte/schema: `basedosdados.br_me_cnpj.{estabelecimentos,empresas}`.
  - situacao_cadastral = '2' -> ativa ; '8' -> baixada
  - coluna `data` = data da fotografia (snapshot); fixamos no MAX para não
    duplicar CNPJs entre extrações.
  - cnae_fiscal_principal = subclasse de 7 dígitos (string); casamos por prefixo
    de classe/grupo com STARTS_WITH após LPAD (defensivo contra zero à esquerda).

Uso:
  export GCP_PROJECT=seu-projeto-faturado          # projeto GCP com billing
  gcloud auth application-default login             # ou service account
  pip install google-cloud-bigquery
  python aggregate_cnpj.py

Custo: as queries varrem a tabela de estabelecimentos filtrando por snapshot e
CNAE. Rode com moderação (mensal, quando a RFB atualiza).
"""

import os, json, datetime as dt
from google.cloud import bigquery

OUT_PATH = "data.json"
EST = "`basedosdados.br_me_cnpj.estabelecimentos`"
EMP = "`basedosdados.br_me_cnpj.empresas`"

client = bigquery.Client(project=os.environ.get("GCP_PROJECT"))

# ------------------------------------------------------------------ mapeamento setor -> classes CNAE
# (prefixo, rótulo, código exibido). Validado contra IBGE/Concla (CNAE 2.3).
# Agro é aproximado: sementes/biofertilizantes não têm CNAE limpo (ver README).
SECTOR_CNAE = {
    "saude": [
        ("8610", "Hospitais",                 "86.10"),
        ("8630", "Clínicas / ambulatorial",   "86.30"),
        ("8640", "Diagnóstico (labs, imagem)","86.40"),
        ("8650", "Profissionais de saúde",    "86.50"),
        ("8660", "Apoio à gestão de saúde",   "86.60"),
        ("8690", "Outras atenção à saúde",    "86.90"),
        ("212",  "Fabricação de medicamentos","21.2x"),
    ],
    "educacao": [
        ("851",  "Infantil / fundamental",    "85.1x"),
        ("8520", "Ensino médio",              "85.20"),
        ("853",  "Educação superior",         "85.3x"),
        ("854",  "Educação profissional",     "85.4x"),
        ("8550", "Apoio à educação",          "85.50"),
        ("8599", "Outras (cursos, prep.)",    "85.99"),
    ],
    "agro": [
        ("2013", "Adubos e fertilizantes",    "20.13"),
        ("2051", "Defensivos agrícolas",      "20.51"),
        ("4683", "Atacado defensivos/adubos", "46.83"),
        ("4623", "Atacado insumos agropec.",  "46.23"),
    ],
}

PORTE_MAP = {  # códigos RFB (com/sem zero à esquerda) e possíveis descrições
    "1": "ME", "01": "ME", "micro empresa": "ME",
    "3": "EPP", "03": "EPP", "empresa de pequeno porte": "EPP",
    "5": "DEMAIS", "05": "DEMAIS", "demais": "DEMAIS",
}

# ------------------------------------------------------------------ SQL builders
def _cases(buckets, field):
    idx = 2 if field == "code" else 1   # bucket = (prefixo, rótulo, código)
    whens = "\n    ".join(
        f"WHEN STARTS_WITH(cnae7, '{b[0]}') THEN '{b[idx]}'" for b in buckets
    )
    return f"CASE\n    {whens}\n  END"

def _filter(buckets):
    return " OR ".join(f"STARTS_WITH(cnae7, '{b[0]}')" for b in buckets)

def sql_buckets(buckets):
    return f"""
WITH base AS (
  SELECT LPAD(CAST(cnae_fiscal_principal AS STRING),7,'0') AS cnae7,
         situacao_cadastral AS sit, data_inicio_atividade AS dt_ini,
         data_situacao_cadastral AS dt_sit
  FROM {EST}
  WHERE data = (SELECT MAX(data) FROM {EST})
)
SELECT
  {_cases(buckets,'label')} AS label,
  {_cases(buckets,'code')}  AS code,
  COUNTIF(sit = '2')                                                                   AS ativas,
  COUNTIF(dt_ini >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))                        AS aberturas,
  COUNTIF(sit = '8' AND dt_sit >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))          AS baixas
FROM base
WHERE {_filter(buckets)}
GROUP BY label, code
HAVING label IS NOT NULL
ORDER BY ativas DESC
"""

def sql_porte_capital(buckets):
    return f"""
WITH base AS (
  SELECT LPAD(CAST(e.cnae_fiscal_principal AS STRING),7,'0') AS cnae7,
         e.situacao_cadastral AS sit,
         SAFE_CAST(emp.capital_social AS FLOAT64) AS capital,
         LOWER(CAST(emp.porte AS STRING)) AS porte
  FROM {EST} e
  JOIN {EMP} emp ON e.cnpj_basico = emp.cnpj_basico
  WHERE e.data = (SELECT MAX(data) FROM {EST})
    AND emp.data = (SELECT MAX(data) FROM {EMP})
)
SELECT porte, COUNT(*) AS n, AVG(capital) AS cap_medio
FROM base
WHERE sit = '2' AND ({_filter(buckets)})
GROUP BY porte
"""

def sql_snapshot():
    return f"SELECT MAX(data) AS snap FROM {EST}"

# ------------------------------------------------------------------ run + build estrutura
def build_estrutura(sector_id):
    buckets = SECTOR_CNAE[sector_id]

    rows = list(client.query(sql_buckets(buckets)).result())
    cnae, ativas, aberturas, baixas = [], 0, 0, 0
    for r in rows:
        cnae.append([r["code"], r["label"], int(r["ativas"]), int(r["aberturas"]), int(r["baixas"])])
        ativas    += int(r["ativas"])
        aberturas += int(r["aberturas"])
        baixas    += int(r["baixas"])

    prt = list(client.query(sql_porte_capital(buckets)).result())
    porte_ct = {"ME": 0, "EPP": 0, "DEMAIS": 0}
    cap_sum, cap_n = 0.0, 0
    for r in prt:
        bucket = PORTE_MAP.get((r["porte"] or "").strip(), "DEMAIS")
        porte_ct[bucket] += int(r["n"])
        if r["cap_medio"] is not None:
            cap_sum += float(r["cap_medio"]) * int(r["n"]); cap_n += int(r["n"])
    tot = sum(porte_ct.values()) or 1
    porte = {k: round(v / tot, 2) for k, v in porte_ct.items()}
    capital_medio = round(cap_sum / cap_n) if cap_n else 0

    snap = list(client.query(sql_snapshot()).result())[0]["snap"]
    fonte = f"RFB/CNPJ · {snap.strftime('%b/%Y') if snap else 'n.d.'}"

    return {
        "fonte": fonte, "ativas": ativas,
        "aberturas12m": aberturas, "baixas12m": baixas,
        "capitalMedio": capital_medio, "porte": porte, "cnae": cnae,
    }

# ------------------------------------------------------------------ merge no data.json
def main():
    if not os.path.exists(OUT_PATH):
        raise SystemExit("data.json não existe — rode backfill_news.py antes (ou crie o esqueleto).")
    doc = json.load(open(OUT_PATH, encoding="utf-8"))
    by_id = {s["id"]: s for s in doc["setores"]}

    for sid in SECTOR_CNAE:
        if sid not in by_id:
            print(f"  ! setor {sid} ausente no data.json — pulando"); continue
        print(f"=== {sid} ===")
        est = build_estrutura(sid)
        by_id[sid]["estrutura"] = est
        print(f"  ativas={est['ativas']:,}  líq12m={est['aberturas12m']-est['baixas12m']:+,}  "
              f"cap.médio=R${est['capitalMedio']:,}  ({est['fonte']})".replace(",", "."))

    doc["meta"]["baseCNPJ"] = next(iter(by_id.values()))["estrutura"].get("fonte", doc["meta"].get("baseCNPJ"))
    json.dump(doc, open(OUT_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n✓ {OUT_PATH} atualizado com o eixo estrutural.")


if __name__ == "__main__":
    main()
