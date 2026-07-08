#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RadarPE 2.0 — backfill de notícias (6 meses) via GDELT DOC API + classificação Haiku.

Fluxo:
  1. Para cada setor, consulta a GDELT DOC API (pública, sem chave) em janelas
     semanais cobrindo ~180 dias, restrito a fontes do Brasil.
  2. Deduplica por hash de título normalizado + bucket de data (48h).
  3. Classifica os títulos em lote com o Claude Haiku: confirma relevância,
     tipo de evento (taxonomia fechada), empresas citadas e valor da transação.
  4. Agrega em contagem diária -> série mensal, count(30d), avg90, momentum, trend.
  5. Emite data.json no MESMO contrato que o index.html espera (bloco `news` e `feed`
     por setor). Os blocos `estrutura`, `fonteSetorial` e `teses` vêm da config
     estática / do job de CNPJ e são preservados via merge.

Uso:
  export ANTHROPIC_API_KEY=sk-ant-...
  pip install requests anthropic
  python backfill_news.py                 # roda os 6 meses e grava data.json

Nota sobre profundidade: a DOC API tem janela prática ~3 meses. Para os meses
mais antigos, se as consultas voltarem vazias, use o fallback via GDELT GKG no
BigQuery (SQL no fim deste arquivo, em FALLBACK_GKG_SQL) — mesmo BigQuery do job CNPJ.
"""

import os, re, json, time, hashlib, datetime as dt
from collections import defaultdict
import requests
import anthropic

# ------------------------------------------------------------------ config
GDELT_URL   = "https://api.gdeltproject.org/api/v2/doc/doc"
WINDOW_DAYS = 180
CHUNK_DAYS  = 7          # janela por request (evita truncamento de 250 registros)
MAXRECORDS  = 250
SLEEP_GDELT = 5.0        # respeitar rate limit da DOC API (~1 req / 5s)
HAIKU_MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE  = 20         # títulos por chamada de classificação
OUT_PATH    = "data.json"

client = anthropic.Anthropic()  # lê ANTHROPIC_API_KEY do ambiente

# Setores-piloto. `query` = expressão GDELT (keywords PT + sourcecountry:BR).
# `estrutura`/`fonteSetorial`/`teses` são placeholders preservados no merge —
# o job de CNPJ preenche `estrutura`. Mantenha em sincronia com o index.html.
SECTORS = [
    {
        "id": "saude", "nome": "Saúde", "cor": "#2FA98E",
        "query": '(saúde OR hospital OR clínica OR "plano de saúde" OR operadora OR laboratório OR farmacêutica OR diagnóstico) sourcecountry:BR',
    },
    {
        "id": "educacao", "nome": "Educação", "cor": "#123FD6",
        "query": '(educação OR ensino OR faculdade OR universidade OR edtech OR "educação superior" OR escola) sourcecountry:BR',
    },
    {
        "id": "agro", "nome": "Agro", "cor": "#6DBE45",
        "query": '(agro OR agronegócio OR sementes OR fertilizante OR biofertilizante OR defensivo OR insumos OR "saúde animal") sourcecountry:BR',
    },
]

EVENT_TYPES = ["M&A", "Captação", "IPO", "Distress", "Regulatório", "Resultado", "Tese"]

# ------------------------------------------------------------------ 1. fetch GDELT
def gdelt_window(query, start, end):
    """Retorna lista de artigos (dicts) da DOC API numa janela [start, end)."""
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": MAXRECORDS,
        "sort": "DateAsc",
        "startdatetime": start.strftime("%Y%m%d%H%M%S"),
        "enddatetime":   end.strftime("%Y%m%d%H%M%S"),
    }
    try:
        r = requests.get(GDELT_URL, params=params, timeout=60)
        if r.status_code != 200 or not r.text.strip().startswith("{"):
            return []
        return r.json().get("articles", [])
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"  ! GDELT falhou ({e}) — janela {start:%d/%m}")
        return []

def fetch_sector(sec):
    """Varre 180 dias em janelas semanais. Retorna artigos brutos deduplicados."""
    now = dt.datetime.utcnow()
    start0 = now - dt.timedelta(days=WINDOW_DAYS)
    seen, out = set(), []
    cur = start0
    while cur < now:
        nxt = min(cur + dt.timedelta(days=CHUNK_DAYS), now)
        arts = gdelt_window(sec["query"], cur, nxt)
        for a in arts:
            title = (a.get("title") or "").strip()
            seen_date = a.get("seendate", "")           # ex 20260215T103000Z
            if not title or not seen_date:
                continue
            # dedup: título normalizado + bucket de 2 dias
            norm = re.sub(r"\W+", "", title.lower())
            day  = seen_date[:8]
            bucket = f"{norm[:60]}|{day[:6]}{int(day[6:8])//2:02d}"
            h = hashlib.md5(bucket.encode()).hexdigest()
            if h in seen:
                continue
            seen.add(h)
            out.append({
                "title": title,
                "date": _parse_date(seen_date),
                "source": a.get("domain", ""),
                "url": a.get("url", ""),
            })
        print(f"  {sec['id']}: {cur:%d/%m}–{nxt:%d/%m} -> {len(arts)} art (acum {len(out)})")
        cur = nxt
        time.sleep(SLEEP_GDELT)
    return out

def _parse_date(s):
    try:
        return dt.datetime.strptime(s[:15], "%Y%m%dT%H%M%S").date().isoformat()
    except ValueError:
        return dt.date.today().isoformat()

# ------------------------------------------------------------------ 2. classify (Haiku)
CLASSIFY_SYS = (
    "Você classifica manchetes de notícias brasileiras para uma gestora de Private Equity. "
    "Para cada manchete, devolva: relevante (bool — é notícia de negócio/mercado do setor, "
    "não coluna genérica), tipo (um de: " + ", ".join(EVENT_TYPES) + "), empresas (lista de "
    "nomes citados, ou vazio), valor (valor da transação se explícito, ex 'R$ 180M', senão ''). "
    "Responda APENAS um array JSON, um objeto por manchete, na ordem recebida, sem markdown."
)

def classify_batch(titles):
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))
    try:
        msg = client.messages.create(
            model=HAIKU_MODEL, max_tokens=1500,
            system=CLASSIFY_SYS,
            messages=[{"role": "user", "content": numbered}],
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r"^```json|```$", "", raw).strip()
        arr = json.loads(raw)
        # normaliza tamanho
        while len(arr) < len(titles):
            arr.append({"relevante": True, "tipo": "Tese", "empresas": [], "valor": ""})
        return arr[:len(titles)]
    except (anthropic.APIError, json.JSONDecodeError, IndexError) as e:
        print(f"  ! classificação falhou ({e}) — assumindo Tese")
        return [{"relevante": True, "tipo": "Tese", "empresas": [], "valor": ""} for _ in titles]

def classify_all(arts):
    for i in range(0, len(arts), BATCH_SIZE):
        chunk = arts[i:i+BATCH_SIZE]
        labels = classify_batch([a["title"] for a in chunk])
        for a, lab in zip(chunk, labels):
            a["relevante"] = bool(lab.get("relevante", True))
            a["tipo"]      = lab.get("tipo") if lab.get("tipo") in EVENT_TYPES else "Tese"
            a["empresas"]  = lab.get("empresas", []) or []
            a["valor"]     = lab.get("valor", "") or ""
    return [a for a in arts if a.get("relevante")]

# ------------------------------------------------------------------ 3. aggregate
def aggregate(arts):
    """Deriva o bloco `news` (momentum) e o `feed` a partir dos artigos classificados."""
    today = dt.date.today()
    by_day, by_month, eventos = defaultdict(int), defaultdict(int), defaultdict(int)
    for a in arts:
        d = dt.date.fromisoformat(a["date"])
        by_day[d] += 1
        by_month[d.strftime("%Y-%m")] += 1
        eventos[a["tipo"]] += 1

    def win_sum(d0, d1):  # soma de artigos em [today-d1, today-d0)
        return sum(v for d, v in by_day.items()
                   if today - dt.timedelta(days=d1) <= d < today - dt.timedelta(days=d0))

    count30 = win_sum(0, 30)
    avg90   = round(win_sum(30, 120) / 3) or 1          # média de 30d nos 90d anteriores
    momentum = round(count30 / avg90 - 1, 2)
    trend = "up" if momentum > 0.1 else "down" if momentum < -0.1 else "flat"

    # série mensal (últimos 6 meses, ordem cronológica) p/ o gráfico de barras
    meses_pt = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
    serie = []
    for k in range(5, -1, -1):
        m = (today.replace(day=1) - dt.timedelta(days=30*k))
        key = m.strftime("%Y-%m")
        serie.append([meses_pt[m.month-1], by_month.get(key, 0)])

    # feed: 8 mais recentes, no formato [date, source, title, tipo, [empresas], valor]
    feed = sorted(arts, key=lambda a: a["date"], reverse=True)[:8]
    feed_rows = [[
        dt.date.fromisoformat(a["date"]).strftime("%d/%m"),
        a["source"], a["title"], a["tipo"], a["empresas"], a["valor"]
    ] for a in feed]

    news = {
        "count": count30, "avg90": avg90, "momentum": momentum, "trend": trend,
        "serie": serie, "eventos": dict(eventos),
    }
    return news, feed_rows

# ------------------------------------------------------------------ 4. merge + emit
def load_existing():
    """Preserva estrutura/fonteSetorial/teses já existentes em data.json, se houver."""
    if os.path.exists(OUT_PATH):
        try:
            return {s["id"]: s for s in json.load(open(OUT_PATH))["setores"]}
        except (KeyError, json.JSONDecodeError):
            pass
    return {}

def main():
    prev = load_existing()
    setores = []
    for sec in SECTORS:
        print(f"\n=== {sec['nome']} ===")
        arts = fetch_sector(sec)
        arts = classify_all(arts)
        news, feed = aggregate(arts)
        base = prev.get(sec["id"], {})
        setores.append({
            "id": sec["id"], "nome": sec["nome"], "cor": sec["cor"],
            "news": news, "feed": feed,
            # preservados (job CNPJ / config estática):
            "estrutura":     base.get("estrutura", {}),
            "fonteSetorial": base.get("fonteSetorial", {}),
            "teses":         base.get("teses", []),
        })
        print(f"  -> count30={news['count']} avg90={news['avg90']} momentum={news['momentum']:+.0%} ({news['trend']})")

    out = {
        "meta": {
            "geradoEm": dt.date.today().strftime("%d/%m/%Y"),
            "janela": "últimos 30 dias",
            "baseCNPJ": "RFB/CNPJ · jan/2026",
            "fonteNoticias": "GDELT DOC API · backfill 180d",
        },
        "setores": setores,
    }
    json.dump(out, open(OUT_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n✓ {OUT_PATH} gravado — {sum(len(s['feed']) for s in setores)} itens de feed.")


# ------------------------------------------------------------------ FALLBACK (GKG / BigQuery)
# Se a DOC API vier vazia nos meses mais antigos, rode isto no BigQuery (mesmo projeto
# do job de CNPJ) e alimente `arts` com o resultado (title, date, source, url):
FALLBACK_GKG_SQL = r"""
-- histórico profundo via GDELT GKG (cobre desde 2015, particionado por data)
SELECT
  DATE(_PARTITIONTIME)                    AS date,
  SourceCommonName                        AS source,
  DocumentIdentifier                      AS url,
  V2Themes, V2Persons, V2Organizations
FROM `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE DATE(_PARTITIONTIME) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
  AND SourceCommonName IN ('valor.com.br','braziljournal.com','neofeed.com.br')  -- ajuste às suas fontes
  AND (LOWER(V2Themes) LIKE '%health%' OR LOWER(V2Organizations) LIKE '%saude%') -- por setor
"""

if __name__ == "__main__":
    main()
