# Vivara Price Tracker

Acompanhamento semanal de precificação de SKUs — Vivara e Life by Vivara.
Replica a metodologia do BTG Jewelry Index (Sector Note, Abril 2026).

## Estrutura

```
vivara_tracker/
├── scraper.py        # coleta snapshot de preços
├── analyzer.py       # compara snapshots e gera tabelas
├── requirements.txt
└── data/
    ├── snapshots/    # parquet por data de coleta
    │   └── YYYY-MM-DD_vivara_skus.parquet
    └── reports/      # CSVs exportados
        └── YYYY-MM-DD_*.csv
```

## Setup

```bash
pip install -r requirements.txt
```

## Uso

### 1. Coleta semanal (toda semana no mesmo dia, ex: domingo)

```bash
# Coleta completa (~6k produtos, ~15 min)
python scraper.py

# Testar antes (apenas primeira página por categoria)
python scraper.py --dry-run

# Coletar só uma categoria
python scraper.py --cats Rings Earrings
```

### 2. Análise (após ter ao menos 1 snapshot)

```bash
# Análise do dia com export de CSVs
python analyzer.py --export

# Forçar data base específica
python analyzer.py --date 2026-04-27 --export
```

### 3. Rotina semanal sugerida (cron)

```bash
# Todo domingo às 8h: coleta + análise + export
0 8 * * 0 cd /path/to/vivara_tracker && python scraper.py && python analyzer.py --export
```

## Outputs

### Disponíveis desde o 1º snapshot

| Arquivo | Conteúdo |
|---|---|
| `YYYY-MM-DD_ticket.csv` | Min/Avg/Median/Max por marca e categoria |
| `YYYY-MM-DD_giftability.csv` | % SKUs por faixa de preço |
| `YYYY-MM-DD_descontos.csv` | % SKUs em desconto e desconto médio |

### Disponíveis a partir do 2º snapshot (semana seguinte)

| Arquivo | Conteúdo |
|---|---|
| `YYYY-MM-DD_ajustes_WoW.csv` | Variação vs semana anterior |
| `YYYY-MM-DD_ajustes_MoM.csv` | Variação vs 4 semanas atrás |

## Tabela principal (replica BTG Table 1)

```
Dimensão            Vivara Avg.adj  Vivara Wtd.adj  Life Avg.adj  Life Wtd.adj
Consolidated              +2.8%          +0.7%          -1.4%        -1.4%
--- Categories ---
Rings                     +2.1%          +0.9%          -1.8%        -1.9%
Earrings                  +2.7%          +0.6%          -1.6%        -1.6%
...
--- Materials ---
Gold                      +3.1%          +1.1%             -             -
Silver                    +0.0%          +0.0%          -1.9%        -1.8%
--- Price range ---
R$0-300                   +0.0%          +0.0%          -2.0%        -1.5%
...
```

## Notas metodológicas

- **Nível de granularidade**: SKU (não produto) — alinhado ao BTG (~14k SKUs Vivara, ~8k Life)
- **Preço base para ajuste**: `preco_tabela` (ListPrice) — isola política de preço da variação de desconto
- **SKUs monitorados**: apenas os disponíveis em estoque em ambos os snapshots
- **Wtd.adj**: ponderado pelo preço de tabela do snapshot anterior (mais antigo)
- **WoW**: snapshot mais próximo de t-7 dias (tolerância ±3 dias)
- **MoM**: snapshot mais próximo de t-28 dias (tolerância ±3 dias)
- **Duplicatas**: SKUs que aparecem em múltiplas subcategorias são deduplicados (keep first)
