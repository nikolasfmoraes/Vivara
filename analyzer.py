"""
vivara_tracker/analyzer.py
Compara dois snapshots e gera as tabelas de análise estilo BTG.

Uso:
    python analyzer.py                          # WoW (hoje vs 7d) + MoM (hoje vs 28d)
    python analyzer.py --date 2026-04-27        # forçar data base
    python analyzer.py --export                 # salva CSVs em data/reports/
    python analyzer.py --no-pandora              # rodar sem Pandora (só Vivara+Life)

Outputs gerados:
  1. Tabela de ajustes (replica Table 1 do BTG):
     - Por categoria (Rings, Earrings, etc.)
     - Por material (Gold, Silver)
     - Por faixa de preço
     - Colunas: avg_adj (%), wtd_adj (%)
     - Linhas: WoW e MoM

  2. Tabela de ticket (disponível desde o 1º snapshot):
     - Min, Avg, Median, Max por categoria e marca

  3. Giftability curve (desde o 1º snapshot):
     - % de SKUs disponíveis por faixa de preço

  4. Desconto summary:
     - % SKUs em desconto e desconto médio por marca
"""

import pandas as pd
import numpy as np
import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SNAPSHOTS_DIR = Path("data/snapshots")
REPORTS_DIR   = Path("data/reports")

CATEGORIAS_BTG = ["Rings", "Earrings", "Pendants/Charms", "Chains", "Bracelets", "Necklaces"]
MATERIAIS_BTG  = ["Gold", "Silver"]
FAIXAS_BTG     = ["R$0-300", "R$300-1000", "R$1000-3000", "R$3000-10000", "R$10000+"]
MARCAS              = ["Vivara", "Life"]
MARCAS_COM_PANDORA  = ["Vivara", "Life", "Pandora", "MonteCarlo", "Jolie"]

# ── I/O ───────────────────────────────────────────────────────────────────────

def find_snapshot(target_date: date, fonte: str = "vivara") -> Path | None:
    """
    Encontra snapshot mais próximo de target_date (tolerância ±3 dias).
    fonte: "vivara" (vivara+life) | "pandora"
    """
    mapa = {"vivara": "vivara_skus", "pandora": "pandora_skus", "montecarlo": "montecarlo_skus"}
    nome_arquivo = mapa.get(fonte, f"{fonte}_skus")
    for delta in range(4):
        for sinal in [0, -1, 1, -2, 2, -3, 3]:
            d = target_date + timedelta(days=sinal * (delta == 0 and 0 or delta))
            for ext in [".csv.gz", ".csv"]:
                p = SNAPSHOTS_DIR / f"{d.isoformat()}_{nome_arquivo}{ext}"
                if p.exists():
                    return p
    return None

def load_snapshot(target_date: date, fonte: str = "vivara") -> pd.DataFrame | None:
    path = find_snapshot(target_date, fonte)
    if path is None:
        return None
    log.info(f"  Carregando: {path.name}")
    df = pd.read_csv(path)
    # Filtro de segurança: garantir que só SKUs com estoque entram na análise.
    # Snapshots novos já vêm filtrados na coleta; este filtro protege snapshots legados.
    if "disponivel" in df.columns:
        df = df[df["disponivel"] == True]
    return df.copy()

def find_primeiro_snapshot(fonte: str = "vivara") -> tuple[date, date] | None:
    """
    Encontra o snapshot mais antigo disponível na pasta (= semana 1 do acompanhamento).
    Retorna (data_vivara, data_pandora) ou None se não houver nenhum.
    Para Vivara: busca *_vivara_skus.* e pega o mais antigo.
    Para Pandora: busca *_pandora_skus.* e pega o mais antigo.
    """
    import re
    padrao = re.compile(r"(\d{4}-\d{2}-\d{2})_vivara_skus")
    datas = []
    for f in SNAPSHOTS_DIR.iterdir():
        m = padrao.match(f.name)
        if m:
            try:
                datas.append(date.fromisoformat(m.group(1)))
            except ValueError:
                pass
    if not datas:
        return None
    return min(datas)

def find_primeiro_snapshot_pandora() -> date | None:
    """Encontra o snapshot Pandora mais antigo disponível."""
    import re
    padrao = re.compile(r"(\d{4}-\d{2}-\d{2})_pandora_skus")
    datas = []
    for f in SNAPSHOTS_DIR.iterdir():
        m = padrao.match(f.name)
        if m:
            try:
                datas.append(date.fromisoformat(m.group(1)))
            except ValueError:
                pass
    return min(datas) if datas else None

def load_snapshot_completo(target_date: date, com_pandora: bool = True) -> pd.DataFrame | None:
    """
    Carrega e faz merge dos snapshots Vivara + Pandora em um único DataFrame.
    Se o snapshot da Pandora não existir, avisa mas continua só com Vivara.
    """
    df_viv = load_snapshot(target_date, "vivara")
    if df_viv is None:
        return None

    if not com_pandora:
        return df_viv

    colunas_base = ["sku_id","product_id","marca","categoria_btg","material_btg",
                    "preco_tabela","preco_atual","em_desconto","pct_desconto",
                    "disponivel","faixa_preco","data_coleta"]

    dfs = [df_viv]
    resumo = [f"{len(df_viv)} Vivara+Life"]

    for fonte, label in [("pandora", "Pandora"), ("montecarlo", "MonteCarlo")]:
        df_extra = load_snapshot(target_date, fonte)
        if df_extra is None:
            log.warning(f"  Snapshot {label} não encontrado — continuando sem ele")
        else:
            dfs.append(df_extra)
            resumo.append(f"{len(df_extra)} {label}")

    # Garantir colunas compatíveis antes do concat
    for df_part in dfs:
        for col in colunas_base:
            if col not in df_part.columns:
                df_part[col] = None

    df = pd.concat([d[colunas_base] for d in dfs], ignore_index=True)
    log.info(f"  Merge completo: {' + '.join(resumo)} = {len(df)} SKUs")
    return df

# ── Cálculo de ajustes ────────────────────────────────────────────────────────

def calcular_ajuste(df_novo: pd.DataFrame, df_ant: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada SKU presente nos dois snapshots, calcula a variação de preço efetivo.

    Usa preco_atual (Price) — o preço efetivamente pago pelo cliente — em ambos
    os snapshots. Motivação:
      1. A margem bruta é calculada sobre o preço de venda, não sobre o preço
         de tabela. Ajustes sobre preco_tabela ignoram o efeito promocional.
      2. Marcas com alto percentual de SKUs em promoção teriam distorção
         sistemática na comparação com marcas que promovem pouco, se usássemos
         apenas o preço de tabela.

    Métricas calculadas:
    - avg_adj:  variação simples — média das variações individuais por SKU
    - wtd_adj:  variação ponderada pelo preco_atual do snapshot anterior
                (proxy de receita — itens que geraram mais receita têm mais peso)
    """
    # Merge pelos SKUs em comum — apenas SKUs disponíveis em ambas as datas
    merged = df_novo[["sku_id", "preco_atual", "marca", "categoria_btg", "material_btg", "faixa_preco"]].merge(
        df_ant[["sku_id", "preco_atual"]].rename(columns={"preco_atual": "preco_atual_ant"}),
        on="sku_id", how="inner"
    )

    # Filtrar SKUs com preço efetivo válido nos dois snapshots
    merged = merged[(merged["preco_atual_ant"] > 0) & (merged["preco_atual"] > 0)].copy()
    merged["var_pct"] = (merged["preco_atual"] - merged["preco_atual_ant"]) / merged["preco_atual_ant"]

    return merged

def aggregate_ajustes(merged: pd.DataFrame, group_col: str, valid_values: list) -> pd.DataFrame:
    """
    Agrega variações para um conjunto de SKUs por dimensão (categoria, material, faixa).
    Retorna DataFrame com avg_adj e wtd_adj.
    """
    rows = []
    for val in valid_values:
        sub = merged[merged[group_col] == val]
        if len(sub) == 0:
            rows.append({"group": val, "n_skus": 0, "avg_adj": None, "wtd_adj": None})
            continue
        
        # SKUs com preço ajustado (qualquer direção) e só disponíveis em ambos snapshots
        n_ajustados = (sub["var_pct"] != 0).sum()
        pct_ajustados = n_ajustados / len(sub) * 100

        avg_adj = sub["var_pct"].mean() * 100
        # Wtd adj: peso = preço de tabela anterior (como BTG)
        wtd_adj = np.average(sub["var_pct"], weights=sub["preco_tabela_ant"]) * 100

        rows.append({
            "group":         val,
            "n_skus_total":  len(sub),
            "n_ajustados":   n_ajustados,
            "pct_ajustados": round(pct_ajustados, 1),
            "avg_adj":       round(avg_adj, 2),
            "wtd_adj":       round(wtd_adj, 2),
        })
    return pd.DataFrame(rows).set_index("group")

def tabela_ajustes_btg(df_novo: pd.DataFrame, df_ant: pd.DataFrame, periodo: str) -> pd.DataFrame:
    """
    Gera a tabela completa de ajustes replicando o formato BTG.
    Colunas: Vivara avg_adj, Vivara wtd_adj, Life avg_adj, Life wtd_adj, Pandora avg_adj, Pandora wtd_adj
    Linhas: Consolidated + categorias + materiais + faixas
    """
    all_rows = []

    # Para cada marca (incluindo Pandora se presente)
    marcas_presentes = [m for m in MARCAS_COM_PANDORA if m in df_novo["marca"].unique()]
    marca_dfs = {}
    for marca in marcas_presentes:
        novo_m = df_novo[df_novo["marca"] == marca]
        ant_m  = df_ant[df_ant["marca"] == marca]
        if len(novo_m) == 0 or len(ant_m) == 0:
            continue
        merged_m = calcular_ajuste(novo_m, ant_m)
        marca_dfs[marca] = merged_m

    def get_stats(merged: pd.DataFrame | None) -> tuple:
        if merged is None or len(merged) == 0:
            return (None, None)
        avg = round(merged["var_pct"].mean() * 100, 2)
        wtd = round(np.average(merged["var_pct"], weights=merged["preco_atual_ant"]) * 100, 2)
        return (avg, wtd)

    def build_row(label: str, vivara_merged, life_merged, pandora_merged=None, mc_merged=None, jolie_merged=None) -> dict:
        va, vw = get_stats(vivara_merged)
        la, lw = get_stats(life_merged)
        pa, pw = get_stats(pandora_merged)
        ma, mw = get_stats(mc_merged)
        ja, jw = get_stats(jolie_merged)
        return {
            "Dimensão":           label,
            "Vivara Avg.adj":     f"{va:+.1f}%" if va is not None else "-",
            "Vivara Wtd.adj":     f"{vw:+.1f}%" if vw is not None else "-",
            "Life Avg.adj":       f"{la:+.1f}%" if la is not None else "-",
            "Life Wtd.adj":       f"{lw:+.1f}%" if lw is not None else "-",
            "Pandora Avg.adj":    f"{pa:+.1f}%" if pa is not None else "-",
            "Pandora Wtd.adj":    f"{pw:+.1f}%" if pw is not None else "-",
            "MonteCarlo Avg.adj": f"{ma:+.1f}%" if ma is not None else "-",
            "MonteCarlo Wtd.adj": f"{mw:+.1f}%" if mw is not None else "-",
            "Jolie Avg.adj":      f"{ja:+.1f}%" if ja is not None else "-",
            "Jolie Wtd.adj":      f"{jw:+.1f}%" if jw is not None else "-",
            "_vivara_avg": va,  "_vivara_wtd": vw,
            "_life_avg":   la,  "_life_wtd":   lw,
            "_pandora_avg": pa, "_pandora_wtd": pw,
            "_mc_avg": ma,      "_mc_wtd": mw,
            "_jolie_avg": ja,   "_jolie_wtd": jw,
        }

    # ── Consolidated ──
    all_rows.append(build_row(
        "Consolidated",
        marca_dfs.get("Vivara"),
        marca_dfs.get("Life"),
        marca_dfs.get("Pandora"),
        marca_dfs.get("MonteCarlo"),
        marca_dfs.get("Jolie")
    ))

    # ── Por material (Gold e Silver) ──
    # Filtra por material dentro de cada marca separadamente.
    # Permite separar ajuste por commodity vs. decisão comercial.
    for mat in MATERIAIS_BTG:
        v = marca_dfs.get("Vivara")
        l = marca_dfs.get("Life")
        p = marca_dfs.get("Pandora")
        mc = marca_dfs.get("MonteCarlo")
        jo = marca_dfs.get("Jolie")
        def filt(df_m, m=mat):
            if df_m is None: return None
            r = df_m[df_m["material_btg"] == m]
            return r if len(r) > 0 else None
        all_rows.append(build_row(mat, filt(v), filt(l), filt(p), filt(mc), filt(jo)))

    df_out = pd.DataFrame(all_rows).fillna("-")
    df_out.insert(0, "Período", periodo)
    return df_out

# ── Tabela de ticket ──────────────────────────────────────────────────────────

def tabela_ticket(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ticket mínimo, médio, mediano e máximo — apenas Total por bandeira.
    Calculado sobre preco_tabela de SKUs disponíveis.
    """
    rows = []
    for marca in MARCAS_COM_PANDORA:
        sub = df[df["marca"] == marca]["preco_tabela"]
        if len(sub) == 0:
            continue
        rows.append({
            "Marca":   marca,
            "Min":     int(sub.min()),
            "Avg":     int(sub.mean()),
            "Median":  int(sub.median()),
            "Max":     int(sub.max()),
            "N_SKUs":  len(sub),
        })
    return pd.DataFrame(rows)

def tabela_mix_material(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mix de material por bandeira — % de SKUs Gold, Silver, Other (Consolidated).
    Uma linha por bandeira. Serve para contextualizar ajustes de preço vs. commodity.
    """
    rows = []
    for marca in MARCAS_COM_PANDORA:
        sub_m = df[df["marca"] == marca]
        total_m = len(sub_m)
        if total_m == 0:
            continue
        for mat in MATERIAIS_BTG + ["Other"]:
            n = len(sub_m[sub_m["material_btg"] == mat])
            rows.append({
                "Marca":   marca,
                "Material": mat,
                "N_SKUs":   n,
                "Pct":      round(n / total_m * 100, 1),
            })
    df_mix = pd.DataFrame(rows)
    pivot = df_mix.pivot_table(
        index="Marca", columns="Material", values="Pct", aggfunc="first"
    ).reset_index()
    for col in MATERIAIS_BTG + ["Other"]:
        if col not in pivot.columns:
            pivot[col] = 0.0
    return pivot[["Marca"] + MATERIAIS_BTG + ["Other"]]

# ── Giftability curve ─────────────────────────────────────────────────────────

def tabela_giftability(df: pd.DataFrame) -> pd.DataFrame:
    """
    % de SKUs disponíveis por faixa de preço (sobre preco_tabela).
    Replica 'Jewelry giftability curve' do BTG.
    """
    rows = []
    for marca in MARCAS_COM_PANDORA:
        sub_m = df[df["marca"] == marca]
        total = len(sub_m)
        if total == 0:
            continue
        for faixa in FAIXAS_BTG:
            n = len(sub_m[sub_m["faixa_preco"] == faixa])
            rows.append({
                "Marca":    marca,
                "Faixa":    faixa,
                "N_SKUs":   n,
                "Pct_SKUs": round(n / total * 100, 1),
            })
    return pd.DataFrame(rows)

# ── Desconto summary ──────────────────────────────────────────────────────────

def tabela_descontos(df: pd.DataFrame) -> pd.DataFrame:
    """
    % SKUs em desconto e desconto médio, por marca e categoria.
    """
    rows = []
    for marca in MARCAS_COM_PANDORA:
        sub_m = df[df["marca"] == marca]
        for cat in CATEGORIAS_BTG:
            sub = sub_m[sub_m["categoria_btg"] == cat]
            if len(sub) == 0:
                continue
            n_desc = sub["em_desconto"].sum()
            avg_desc = sub[sub["em_desconto"]]["pct_desconto"].mean() if n_desc > 0 else 0
            rows.append({
                "Marca":         marca,
                "Categoria":     cat,
                "N_SKUs":        len(sub),
                "N_Desconto":    int(n_desc),
                "Pct_Desconto":  round(n_desc / len(sub) * 100, 1),
                "Avg_Desconto":  round(avg_desc, 1),
            })
        # Consolidated por marca
        n_desc = sub_m["em_desconto"].sum()
        if len(sub_m) == 0:
            continue
        avg_desc = sub_m[sub_m["em_desconto"]]["pct_desconto"].mean() if n_desc > 0 else 0
        rows.append({
            "Marca":         marca,
            "Categoria":     "Consolidated",
            "N_SKUs":        len(sub_m),
            "N_Desconto":    int(n_desc),
            "Pct_Desconto":  round(n_desc / len(sub_m) * 100, 1),
            "Avg_Desconto":  round(avg_desc, 1),
        })
    return pd.DataFrame(rows)


# ── Monte Carlo ───────────────────────────────────────────────────────────────

def monte_carlo_receita(
    df_hoje: pd.DataFrame,
    historico_ajustes: list[pd.DataFrame],
    n_simulacoes: int = 10_000,
    n_semanas: int = 4,
) -> pd.DataFrame:
    """
    Simula o impacto de variações futuras de preço sobre o ticket médio ponderado,
    usando a distribuição histórica de ajustes observados por SKU como base.

    Lógica:
    - Para cada simulação, sorteia n_semanas de ajustes da distribuição histórica
      (bootstrap com reposição sobre os var_pct já observados)
    - Aplica os ajustes cumulativos ao preco_tabela atual de cada SKU
    - Calcula o ticket médio ponderado resultante por marca

    Parâmetros:
    - df_hoje: snapshot atual (base de preços)
    - historico_ajustes: lista de DataFrames retornados por calcular_ajuste()
      (cada um é o resultado de um período de comparação)
    - n_simulacoes: número de cenários simulados
    - n_semanas: horizonte de projeção em semanas

    Retorna DataFrame com percentis do ticket médio ponderado por marca:
    P5, P25, Mediana, P75, P95 e variação % vs. hoje
    """
    if not historico_ajustes:
        return pd.DataFrame()

    # Consolidar todos os var_pct históricos observados por marca
    hist_concat = pd.concat(historico_ajustes, ignore_index=True)

    resultados = []

    for marca in MARCAS_COM_PANDORA:
        df_m = df_hoje[df_hoje["marca"] == marca].copy()
        if len(df_m) == 0:
            continue

        # Distribuição histórica de ajustes desta marca
        var_pct_hist = hist_concat[hist_concat["marca"] == marca]["var_pct"].dropna().values
        if len(var_pct_hist) == 0:
            continue

        # Ticket médio ponderado atual (peso = preco_tabela, proxy de receita)
        ticket_atual = np.average(df_m["preco_tabela"], weights=df_m["preco_tabela"])

        # Monte Carlo: n_simulacoes × n_semanas ajustes sorteados com reposição
        np.random.seed(42)
        # Shape: (n_simulacoes, n_semanas)
        ajustes_sorteados = np.random.choice(var_pct_hist, size=(n_simulacoes, n_semanas), replace=True)
        # Fator cumulativo: produto de (1 + adj) ao longo das n_semanas
        fatores = np.prod(1 + ajustes_sorteados, axis=1)  # shape: (n_simulacoes,)

        # Ticket simulado = ticket_atual * fator
        tickets_sim = ticket_atual * fatores

        # Percentis
        p5, p25, p50, p75, p95 = np.percentile(tickets_sim, [5, 25, 50, 75, 95])

        resultados.append({
            "Marca":          marca,
            "Ticket_hoje":    round(ticket_atual, 0),
            "P5":             round(p5, 0),
            "P25":            round(p25, 0),
            "Mediana":        round(p50, 0),
            "P75":            round(p75, 0),
            "P95":            round(p95, 0),
            "Var_P5_pct":     round((p5 / ticket_atual - 1) * 100, 1),
            "Var_mediana_pct": round((p50 / ticket_atual - 1) * 100, 1),
            "Var_P95_pct":    round((p95 / ticket_atual - 1) * 100, 1),
            "N_simulacoes":   n_simulacoes,
            "N_semanas":      n_semanas,
            "N_obs_hist":     len(var_pct_hist),
        })

    return pd.DataFrame(resultados)


def tabela_ajuste_por_bandeira(df_novo: pd.DataFrame, df_ant: pd.DataFrame, periodo: str) -> pd.DataFrame:
    """
    Tabela simplificada: uma linha por bandeira, só o ajuste consolidado.
    Sem desagregação por categoria, material ou faixa.
    Colunas: Bandeira | N_SKUs | Avg.adj | Wtd.adj
    Linhas: Vivara, Life, Pandora, MonteCarlo, Jolie
    """
    rows = []
    for marca in MARCAS_COM_PANDORA:
        novo_m = df_novo[df_novo["marca"] == marca]
        ant_m  = df_ant[df_ant["marca"] == marca]
        if len(novo_m) == 0 or len(ant_m) == 0:
            continue
        merged = calcular_ajuste(novo_m, ant_m)
        if len(merged) == 0:
            continue
        avg = round(merged["var_pct"].mean() * 100, 2)
        wtd = round(np.average(merged["var_pct"], weights=merged["preco_atual_ant"]) * 100, 2)
        rows.append({
            "Período":   periodo,
            "Bandeira":  marca,
            "N_SKUs":    len(merged),
            "Avg.adj":   f"{avg:+.2f}%",
            "Wtd.adj":   f"{wtd:+.2f}%",
            "_avg":      avg,
            "_wtd":      wtd,
        })
    return pd.DataFrame(rows)

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Vivara tracker — análise de preços")
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Data base (formato YYYY-MM-DD, padrão: hoje)")
    parser.add_argument("--export", action="store_true",
                        help="Exportar tabelas como CSV em data/reports/")
    parser.add_argument("--no-pandora", action="store_true",
                        help="Rodar análise só com Vivara+Life, sem Pandora e MonteCarlo")
    parser.add_argument("--snapshots-dir", default="data/snapshots")
    args = parser.parse_args()

    global SNAPSHOTS_DIR, REPORTS_DIR
    SNAPSHOTS_DIR = Path(args.snapshots_dir)

    base_date   = date.fromisoformat(args.date)
    com_pandora = not args.no_pandora

    # ── Carregar snapshot base (hoje) — merge Vivara+Life+Pandora ──
    log.info(f"Carregando snapshot base: {base_date}")
    df_hoje = load_snapshot_completo(base_date, com_pandora=com_pandora)
    if df_hoje is None:
        log.error(f"Nenhum snapshot encontrado para {base_date}. Execute scraper.py primeiro.")
        return

    log.info(f"  SKUs disponíveis: {len(df_hoje)} | Marcas: {df_hoje['marca'].value_counts().to_dict()}")

    sep = "═" * 70

    # ════════════════════════════════════════════════════════════════════════
    # SEÇÃO 1 — OVERVIEW DAS MARCAS (snapshot atual, sem variação de preço)
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n{sep}")
    print("SEÇÃO 1 — OVERVIEW DAS MARCAS")
    print(sep)

    # 1a. Ticket por bandeira
    print(f"\n{sep}")
    print("TICKET POR BANDEIRA (preco_tabela — SKUs disponíveis)")
    print(sep)
    df_ticket = tabela_ticket(df_hoje)
    print(df_ticket.to_string(index=False))

    # 1b. Giftability curve
    print(f"\n{sep}")
    print("GIFTABILITY — % de SKUs por faixa de preco_tabela")
    print(sep)
    df_gift = tabela_giftability(df_hoje)
    pivot_gift = df_gift.pivot(index="Faixa", columns="Marca", values="Pct_SKUs")
    print(pivot_gift.to_string())

    # 1c. Descontos ativos
    print(f"\n{sep}")
    print("DESCONTOS ATIVOS (preco_atual < preco_tabela)")
    print(sep)
    df_desc = tabela_descontos(df_hoje)
    print(df_desc.to_string(index=False))

    # 1d. Mix de material (só Consolidated)
    print(f"\n{sep}")
    print("MIX DE MATERIAL POR BANDEIRA — Consolidated (% de SKUs)")
    print("Contexto: Gold/Silver por bandeira — use para separar driver")
    print("commodity (ouro/prata) de decisao comercial nos ajustes de preco.")
    print(sep)
    df_mix = tabela_mix_material(df_hoje)
    print(df_mix.to_string(index=False))

    # ════════════════════════════════════════════════════════════════════════
    # SEÇÃO 2 — VARIAÇÃO DE PREÇO (requer snapshot anterior)
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n{sep}")
    print("SEÇÃO 2 — VARIAÇÃO DE PREÇO EFETIVO (preco_atual)")
    print("Metrica: Avg.adj = media simples | Wtd.adj = ponderado por preco_atual")
    print(sep)

    ajuste_tables = []
    cols_display = ["Período", "Dimensão", "Vivara Avg.adj", "Vivara Wtd.adj",
                    "Life Avg.adj", "Life Wtd.adj", "Pandora Avg.adj", "Pandora Wtd.adj",
                    "MonteCarlo Avg.adj", "MonteCarlo Wtd.adj",
                    "Jolie Avg.adj", "Jolie Wtd.adj"]

    def rodar_periodo(label, df_ant):
        df_band = tabela_ajuste_por_bandeira(df_hoje, df_ant, label)
        df_aj   = tabela_ajustes_btg(df_hoje, df_ant, label)
        ajuste_tables.append(df_aj)
        print(f"\n{sep}")
        print(f"AJUSTE POR BANDEIRA — {label}")
        print(sep)
        print(df_band[["Período","Bandeira","N_SKUs","Avg.adj","Wtd.adj"]].to_string(index=False))
        print(f"\n{sep}")
        print(f"AJUSTE CONSOLIDADO + GOLD/SILVER — {label}")
        print(sep)
        print(df_aj[cols_display].to_string(index=False))
        return df_band, df_aj

    df_band_s1 = df_aj_s1 = None
    data_semana1 = find_primeiro_snapshot()
    if data_semana1 is not None and data_semana1 < base_date:
        label_s1 = f"vs. semana 1 ({data_semana1.strftime('%d/%m/%Y')})"
        log.info(f"\nBuscando snapshot semana 1: {data_semana1}")
        df_s1 = load_snapshot_completo(data_semana1, com_pandora=com_pandora)
        if df_s1 is not None:
            log.info(f"  SKUs semana 1: {len(df_s1)}")
            df_band_s1, df_aj_s1 = rodar_periodo(label_s1, df_s1)
        else:
            log.warning("  Snapshot semana 1 não carregado — pulando")
    else:
        log.warning("  Semana 1 = hoje (só um snapshot disponível) — variação indisponível")

    df_band_mom = df_aj_mom = None
    data_mom = base_date - timedelta(days=28)
    log.info(f"\nBuscando snapshot MoM: {data_mom}")
    df_mom = load_snapshot_completo(data_mom, com_pandora=com_pandora)
    if df_mom is not None:
        label_mom = f"MoM (vs {data_mom.strftime('%d/%m/%Y')})"
        log.info(f"  SKUs MoM: {len(df_mom)}")
        df_band_mom, df_aj_mom = rodar_periodo(label_mom, df_mom)
    else:
        log.warning(f"  Snapshot MoM não encontrado — disponível após 28 dias de coleta")

    # ── Export ──
    if args.export:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        base_str = base_date.isoformat()

        # Seção 1
        df_ticket.to_csv(REPORTS_DIR / f"{base_str}_ticket.csv", index=False)
        df_gift.to_csv(REPORTS_DIR / f"{base_str}_giftability.csv", index=False)
        df_desc.to_csv(REPORTS_DIR / f"{base_str}_descontos.csv", index=False)
        df_mix.to_csv(REPORTS_DIR / f"{base_str}_mix_material.csv", index=False)

        # Seção 2
        for df_band, df_aj in [(df_band_s1, df_aj_s1), (df_band_mom, df_aj_mom)]:
            if df_aj is None:
                continue
            slug = df_aj["Período"].iloc[0].replace(" ","_").replace("(","").replace(")","").replace("/","")
            df_aj.to_csv(REPORTS_DIR / f"{base_str}_ajustes_{slug}.csv", index=False)
            df_band.to_csv(REPORTS_DIR / f"{base_str}_bandeiras_{slug}.csv", index=False)

        log.info(f"\nRelatórios exportados em: {REPORTS_DIR}")

if __name__ == "__main__":
    main()
