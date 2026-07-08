# RadarPE 2.0 — versão preliminar

Plataforma de inteligência setorial para Private Equity. Cruza dois eixos por setor:
**momentum de notícias** (o que está em evidência agora) × **estrutura de empresas**
(retrato de fundo via CNPJ/CNAE). Setores-piloto: **Saúde, Educação, Agro**.

Stack: `index.html` estático (GitHub Pages) + dois jobs em Python que produzem um
único `data.json`. Sem build, sem servidor.

```
index.html          front (3 telas: Termômetro, Setores/Detalhe, Notícias)
backfill_news.py    eixo temporal  -> preenche news + feed no data.json (GDELT + Haiku)
aggregate_cnpj.py   eixo estrutural -> preenche estrutura no data.json (BigQuery/Base dos Dados)
data.json           saída dos jobs; o front consome. Se não existir, o front usa dados-semente.
```

## Como funciona o data.json

Os dois jobs escrevem no **mesmo** `data.json` por *merge*: cada um preenche seu bloco e
preserva o do outro. Ordem indiferente. O front carrega `./data.json`; se faltar, cai nos
dados-semente embutidos (nunca quebra numa demo). Contrato por setor:

```jsonc
{
  "id":"saude","nome":"Saúde","cor":"#2FA98E",
  "news":  { "count":.., "avg90":.., "momentum":.., "trend":"up",
             "serie":[["mai",33],..], "eventos":{"M&A":9,..} },   // backfill_news.py
  "feed":  [["01/07","dominio.com","Título..","M&A",["Empresa"],"R$ 180M"], ..], // backfill_news.py
  "estrutura": { "fonte":"RFB/CNPJ · jan/2026","ativas":.., "aberturas12m":..,
                 "baixas12m":.., "capitalMedio":.., "porte":{"ME":.6,"EPP":.2,"DEMAIS":.2},
                 "cnae":[["86.10","Hospitais",7420,240,410], ..] },  // aggregate_cnpj.py
  "fonteSetorial": { .. },   // config estática (ANS/INEP/MAPA)
  "teses": [ .. ]            // config estática (subsegmentos do pipeline)
}
```

## Bring-up (ordem sugerida — validação incremental)

1. **Front no ar com semente.** Suba `index.html` num repo novo (`radarpe-2`) e ligue o
   GitHub Pages. Já funciona com os dados-semente ilustrativos.

2. **Eixo de notícias.**
   ```
   export ANTHROPIC_API_KEY=sk-ant-...
   pip install requests anthropic
   python backfill_news.py          # gera data.json com news + feed (6 meses)
   ```
   Commit do `data.json`. O Termômetro passa a mostrar momentum real; o rodapé indica "live".
   Ajuste as `query` por setor e a lista de fontes conforme o ruído.

3. **Eixo estrutural.**
   ```
   export GCP_PROJECT=seu-projeto-faturado
   gcloud auth application-default login
   pip install google-cloud-bigquery
   python aggregate_cnpj.py         # preenche estrutura no data.json (merge)
   ```
   Commit do `data.json` atualizado. O Detalhe do Setor mostra a base de empresas real.

4. **Confere as duas telas de Detalhe** e só então parta para os demais setores.

## Depois da preliminar (próximos passos)

- **Automatizar** o `backfill_news.py` como Action diária (forward-going), mantendo o
  backfill como execução única de histórico.
- **Demais setores** do pipeline (Serviços Financeiros, Tecnologia, Telecom). Lembrar que
  nesses o CNAE dilui os alvos em códigos genéricos de TI/financeiro — o eixo estrutural
  rende menos; priorizar o de notícias.
- **Fase 2 de sinais:** emissões de debêntures (CVM/ANBIMA) e delta de QSA para watchlists.

## Notas / limitações assumidas

- **DOC API (notícias):** janela prática ~3 meses. Para os meses 4–6, usar o fallback
  `FALLBACK_GKG_SQL` (GDELT GKG no BigQuery) embutido em `backfill_news.py`.
- **CNAE de Agro é aproximado:** sementes e biofertilizantes não têm CNAE próprio; os
  buckets cobrem fabricação de adubos/defensivos e atacado de insumos. Complementar com
  MAPA/CONAB. Saúde e Educação têm CNAE limpo (divisões 86 e 85).
- **Snapshot do CNPJ:** o job fixa `data = MAX(data)` para não duplicar CNPJs entre
  fotografias. Confirmar nomes de colunas no schema da Base dos Dados antes do 1º run
  (`porte` e `capital_social` podem variar de encoding entre versões).
- **Proveniência:** cada número no front carrega fonte + data. Manter isso ao evoluir —
  é o que sustenta o uso institucional.

Fontes de dados: GDELT DOC/GKG · RFB/CNPJ (Base dos Dados) · ANS · INEP · MAPA/CONAB · CVM.
