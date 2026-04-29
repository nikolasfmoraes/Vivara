"""
vivara_tracker/analyzer.py
Compara snapshots e gera tabelas de análise de precificação por SKU.

Uso:
    python analyzer.py                   # análise do dia
    python analyzer.py --date 2026-04-27 # forçar data base
    python analyzer.py --export          # salva CSVs + LaTeX em data/reports/
    python analyzer.py --no-pandora      # só Vivara+Life

Seção 1 - Overview (desde o 1º snapshot):
    ticket.csv          Ticket por bandeira (Min, Avg, Median, Max)
    giftability.csv     % SKUs por faixa de preço
    descontos.csv       % SKUs em promoção e desconto médio
    mix_material.csv    % Gold/Silver por bandeira

Seção 2 - Variação de preço (desde o 2º snapshot):
    bandeiras_{periodo}.csv       Avg.adj e Wtd.adj por bandeira
    ajustes_{periodo}.csv         Consolidated + Gold + Silver por bandeira
    relatorio_{data}.tex          LaTeX com análise real dos dados
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


# ── Geração de LaTeX com dados reais ─────────────────────────────────────────

def _fmt_adj(val: str) -> str:
    """Formata variacao de preco para LaTeX.
    Verde (ForestGreen) = alta de preco — positivo para a empresa.
    Vermelho (BrickRed) = queda de preco — negativo para a empresa.
    """
    if val == "-":
        return r"\textcolor{gray}{---}"
    try:
        n = float(val.replace("%","").replace("+",""))
        val_tex = val.replace("%", r"\%")
        if n > 0:
            return r"\textcolor{ForestGreen}{" + val_tex + r"}"
        elif n < 0:
            return r"\textcolor{BrickRed}{" + val_tex + r"}"
        else:
            return val_tex
    except:
        return val.replace("%", r"\%")

def gerar_latex(
    base_date: date,
    df_hoje: pd.DataFrame,
    df_ticket: pd.DataFrame,
    df_gift: pd.DataFrame,
    df_desc: pd.DataFrame,
    df_mix: pd.DataFrame,
    periodos: list[dict],   # [{"label": str, "df_band": df, "df_aj": df, "df_top": df}, ...]
) -> str:
    """
    Gera um relatório LaTeX completo com os dados reais da análise.
    Estrutura espelha o documentacao_resumida.tex mas com tabelas preenchidas.
    periodos: lista de dicts com label, df_band e df_aj para cada período disponível.
    """
    data_str = base_date.strftime("%d/%m/%Y")
    marcas = [m for m in MARCAS_COM_PANDORA if m in df_hoje["marca"].unique()]

    def latex_escape(s: str) -> str:
        return str(s).replace("$", r"\$").replace("%", r"\%").replace("_", r"\_").replace("&", r"\&")

    # ── TICKET ────────────────────────────────────────────────────────────────
    ticket_rows = ""
    for _, row in df_ticket.iterrows():
        ticket_rows += (
            f"    {latex_escape(row['Marca'])} & "
            f"R\\${row['Min']:,.0f} & "
            f"R\\${row['Avg']:,.0f} & "
            f"R\\${row['Median']:,.0f} & "
            f"R\\${row['Max']:,.0f} & "
            f"{row['N_SKUs']:,} \\\\\n"
        )

    # ── GIFTABILITY ──────────────────────────────────────────────────────────
    gift_pivot = df_gift.pivot(index="Faixa", columns="Marca", values="Pct_SKUs").fillna(0.0)
    faixas_order = ["R$0-300","R$300-1000","R$1000-3000","R$3000-10000","R$10000+"]
    gift_pivot = gift_pivot.reindex([f for f in faixas_order if f in gift_pivot.index])
    gift_cols = [m for m in marcas if m in gift_pivot.columns]
    gift_header = " & ".join([latex_escape(m) for m in gift_cols])
    gift_rows = ""
    for faixa, row in gift_pivot.iterrows():
        vals = " & ".join([f"{row[m]:.1f}\\%" if m in row.index else "---" for m in gift_cols])
        gift_rows += f"    {latex_escape(faixa)} & {vals} \\\\\n"

    # ── DESCONTOS ────────────────────────────────────────────────────────────
    desc_rows = ""
    for _, row in df_desc[df_desc["Categoria"] == "Consolidated"].iterrows():
        desc_rows += (
            f"    {latex_escape(row['Marca'])} & "
            f"{row['N_SKUs']:,} & "
            f"{row['N_Desconto']:,} & "
            f"{row['Pct_Desconto']:.1f}\\% & "
            f"{row['Avg_Desconto']:.1f}\\% \\\\\n"
        )

    # ── MIX MATERIAL ─────────────────────────────────────────────────────────
    mix_rows = ""
    for _, row in df_mix.iterrows():
        gold = row["Gold"] if "Gold" in row else 0.0
        silver = row["Silver"] if "Silver" in row else 0.0
        other = row["Other"] if "Other" in row else 0.0
        mix_rows += (
            f"    {latex_escape(row['Marca'])} & "
            f"{gold:.1f}\\% & "
            f"{silver:.1f}\\% & "
            f"{other:.1f}\\% \\\\\n"
        )

    # ── SEÇÃO 2 ───────────────────────────────────────────────────────────────
    secao2_blocks = ""
    for per in periodos:
        label = latex_escape(per["label"])
        df_band = per["df_band"]
        df_aj   = per["df_aj"]

        # Top SKUs + nota de marcas com 0% de variação
        df_top     = per.get("df_top", pd.DataFrame())
        df_top_all = per.get("df_top_all", pd.DataFrame())  # contém N_mudaram para todas as marcas
        top_block  = ""

        # Identificar marcas que aparecem com 0.00% (avg e wtd) na tabela de bandeiras
        marcas_zero = []
        for _, row in df_band.iterrows():
            try:
                avg_n = float(str(row["Avg.adj"]).replace("%","").replace("+",""))
                wtd_n = float(str(row["Wtd.adj"]).replace("%","").replace("+",""))
                if abs(avg_n) < 0.005 and abs(wtd_n) < 0.005:
                    marcas_zero.append({"Bandeira": row["Bandeira"], "N_SKUs": row["N_SKUs"]})
            except:
                pass

        # Bloco top 3 SKUs (marcas com variação)
        if not df_top.empty:
            top_block = r"\vspace{4pt}" + "\n" + r"{\footnotesize\textit{Top 3 SKUs com maior varia\c{c}\~ao de pre\c{c}o por bandeira:}}" + "\n\n"
            top_block += r"\begin{footnotesize}" + "\n"
            for marca in df_top["Marca"].unique():
                sub = df_top[df_top["Marca"] == marca]
                top_block += f"\\textbf{{{latex_escape(marca)}:}} "
                itens = []
                for _, r2 in sub.iterrows():
                    sinal = "+" if r2["Var_pct"] > 0 else ""
                    cor = "ForestGreen" if r2["Var_pct"] > 0 else "BrickRed"
                    nome_raw = str(r2["Produto"])
                    nome = latex_escape(nome_raw[:50] + ("..." if len(nome_raw) > 50 else ""))
                    itens.append(
                        f"{nome} (R\\${r2['Preco_Ant']:,.0f}$\\to$R\\${r2['Preco_Novo']:,.0f} "
                        f"\\textcolor{{{cor}}}{{{sinal}{r2['Var_pct']:.1f}\\%}})"
                    )
                top_block += "; ".join(itens) + ".\\\\\n"
            top_block += r"\end{footnotesize}" + "\n"

        # Nota ** para marcas com 0.00% — confirma que houve coleta e que o zero é real
        if marcas_zero:
            top_block += r"\vspace{2pt}" + "\n"
            top_block += r"\begin{footnotesize}" + "\n"
            for mz in marcas_zero:
                top_block += (
                    f"\\textbf{{**{latex_escape(mz['Bandeira'])}}}: "
                    f"nenhum SKU alterou pre\\c{{c}}o entre os dois per\\'{{\\'}}iodos "
                    f"({mz['N_SKUs']:,} SKUs comparados --- coleta verificada).\\\\\n"
                )
            top_block += r"\end{footnotesize}" + "\n"

        # Tabela bandeiras
        band_rows = ""
        for _, row in df_band.iterrows():
            avg = _fmt_adj(row["Avg.adj"])
            wtd = _fmt_adj(row["Wtd.adj"])
            band_rows += (
                f"    {latex_escape(row['Bandeira'])} & "
                f"{row['N_SKUs']:,} & {avg} & {wtd} \\\\\n"
            )

        # Tabela ajustes (Consolidated, Gold, Silver)
        # Abreviações para caber na página (5 marcas x 2 cols = 10 colunas)
        ABREV = {
            "Vivara": "Vivara", "Life": "Life", "Pandora": "Pandora",
            "MonteCarlo": "M.Carlo", "Jolie": "Jolie"
        }
        aj_cols = [c for c in df_aj.columns if "Avg.adj" in c or "Wtd.adj" in c]
        marcas_aj = [c.replace(" Avg.adj","") for c in aj_cols if "Avg.adj" in c]

        aj_rows = ""
        dim_col = [c for c in df_aj.columns if "Dimensão" in c or "Dimensao" in c]
        dim_col = dim_col[0] if dim_col else "Dimensão"
        for _, row in df_aj.iterrows():
            dim = latex_escape(str(row.get(dim_col, row.get("Dimensão", ""))))
            cells = ""
            for m in marcas_aj:
                avg_val = row.get(f"{m} Avg.adj", "-")
                wtd_val = row.get(f"{m} Wtd.adj", "-")
                cells += f" & {_fmt_adj(str(avg_val))} & {_fmt_adj(str(wtd_val))}"
            aj_rows += f"    {dim}{cells} \\\\\n"

        n_marcas_aj = len(marcas_aj)
        # Usar @{{}} para remover espaço extra e \scriptsize para caber
        col_spec_aj = "@{}l" + "rr" * n_marcas_aj + "@{}"
        marcas_header_aj = " & ".join(
            [f"\\multicolumn{{2}}{{c}}{{{latex_escape(ABREV.get(m, m))}}}"
             for m in marcas_aj]
        )
        # Avg/Wtd abreviados
        sub_header_aj = " & ".join(["Avg & Wtd"] * n_marcas_aj)

        secao2_blocks += rf"""
\subsection{{{label}}}

\subsubsection{{Ajuste por Bandeira}}

\begin{{table}}[H]
\centering
\begin{{tabular}}{{lrrr}}
\toprule
\textbf{{Bandeira}} & \textbf{{N\_SKUs}} & \textbf{{Avg.adj}} & \textbf{{Wtd.adj}} \\
\midrule
{band_rows}\bottomrule
\end{{tabular}}
\end{{table}}
{top_block}
\subsubsection{{Consolidated + Gold/Silver}}

\begin{{table}}[H]
\centering
{{\scriptsize
\setlength{{\tabcolsep}}{{4pt}}
\begin{{tabular}}{{{col_spec_aj}}}
\toprule
\textbf{{Dim.}} & {marcas_header_aj} \\
 & {sub_header_aj} \\
\midrule
{aj_rows}\bottomrule
\end{{tabular}}
}}
\end{{table}}
"""

    sem2_aviso = ""
    if not periodos:
        sem2_aviso = r"""
\begin{infobox}
Ainda não há snapshots anteriores disponíveis para calcular variação de preço.
Execute o tracker por pelo menos 2 semanas para que esta seção seja preenchida.
\end{infobox}
"""

    n_gift_cols = len(gift_cols)
    col_spec_gift = "l" + "r" * n_gift_cols

    doc = rf"""\documentclass[11pt, a4paper]{{article}}
\usepackage[utf8]{{inputenc}}
\usepackage[T1]{{fontenc}}
\usepackage[brazil]{{babel}}
\usepackage{{geometry}}
\usepackage{{booktabs}}
\usepackage{{array}}
\usepackage[dvipsnames]{{xcolor}}
\usepackage{{fancyhdr}}
\usepackage{{titlesec}}
\usepackage{{parskip}}
\usepackage{{float}}
\usepackage{{mdframed}}
\geometry{{top=2.2cm, bottom=2.2cm, left=2.5cm, right=2.5cm}}
\definecolor{{azulescuro}}{{RGB}}{{15, 40, 80}}
\definecolor{{azulmedio}}{{RGB}}{{30, 80, 160}}
\definecolor{{cinza}}{{RGB}}{{100, 100, 100}}
\definecolor{{cinzaclaro}}{{RGB}}{{240, 240, 240}}
\definecolor{{teal}}{{RGB}}{{0, 120, 80}}
\titleformat{{\section}}{{\color{{azulescuro}}\Large\bfseries}}{{\thesection.}}{{0.7em}}{{}}[\color{{azulmedio}}\rule{{\linewidth}}{{0.8pt}}]
\titleformat{{\subsection}}{{\color{{azulmedio}}\large\bfseries}}{{\thesubsection.}}{{0.6em}}{{}}
\titleformat{{\subsubsection}}{{\color{{cinza}}\normalsize\bfseries}}{{\thesubsubsection.}}{{0.5em}}{{}}
\pagestyle{{fancy}}
\fancyhf{{}}
\fancyhead[L]{{\small\color{{cinza}} Jewelry Price Tracker --- Relatório {data_str}}}
\fancyhead[R]{{\small\color{{cinza}} \today}}
\fancyfoot[C]{{\small\color{{cinza}} \thepage}}
\renewcommand{{\headrulewidth}}{{0.4pt}}
\newmdenv[linecolor=azulmedio, linewidth=1pt, backgroundcolor=cinzaclaro,
  innertopmargin=6pt, innerbottommargin=6pt, innerleftmargin=8pt,
  innerrightmargin=8pt, skipabove=6pt, skipbelow=4pt]{{infobox}}

\begin{{document}}

\begin{{titlepage}}
  \centering
  \vspace*{{3cm}}
  {{\color{{azulescuro}}\Huge\bfseries Jewelry Price Tracker\par}}
  \vspace{{0.5cm}}
  {{\color{{azulmedio}}\Large\bfseries Relatório Semanal\par}}
  \vspace{{1cm}}
  {{\color{{cinza}}\large Data base: {data_str}\par}}
  \vspace{{3cm}}
  \rule{{10cm}}{{0.8pt}}\\[0.5cm]
  {{\large Vivara \textbullet{{}} Life \textbullet{{}} Pandora \textbullet{{}} Monte Carlo \textbullet{{}} Jolie}}
  \vfill
  {{\color{{cinza}}\small Uso interno --- Equity Research\par}}
\end{{titlepage}}

\tableofcontents
\newpage

% =============================================================================
\section{{Seção 1 --- Overview das Marcas}}
% =============================================================================

\subsection{{Ticket por Bandeira}}

Estatísticas de \texttt{{preco\_tabela}} dos SKUs disponíveis.
Permite comparar o posicionamento de ticket entre bandeiras.

\begin{{table}}[H]
\centering
\begin{{tabular}}{{lrrrrr}}
\toprule
\textbf{{Marca}} & \textbf{{Min}} & \textbf{{Avg}} & \textbf{{Median}} & \textbf{{Max}} & \textbf{{N\_SKUs}} \\
\midrule
{ticket_rows}\bottomrule
\end{{tabular}}
\end{{table}}

\subsection{{Giftability Curve}}

Distribuição (\%) dos SKUs disponíveis por faixa de \texttt{{preco\_tabela}}.
Valores maiores em faixas baixas indicam portfólio mais acessível para presentes.

\begin{{table}}[H]
\centering
\begin{{tabular}}{{{col_spec_gift}}}
\toprule
\textbf{{Faixa}} & {gift_header} \\
\midrule
{gift_rows}\bottomrule
\end{{tabular}}
\end{{table}}

\subsection{{Descontos Ativos}}

SKUs com \texttt{{preco\_atual}} $<$ \texttt{{preco\_tabela}} nesta semana.

\begin{{table}}[H]
\centering
\begin{{tabular}}{{lrrrr}}
\toprule
\textbf{{Marca}} & \textbf{{N\_SKUs}} & \textbf{{N\_Desc.}} & \textbf{{Pct\_Desc.}} & \textbf{{Avg\_Desc.}} \\
\midrule
{desc_rows}\bottomrule
\end{{tabular}}
\end{{table}}

{{\small\textit{{*Pandora: a marca sempre registra \texttt{{Price}} = \texttt{{ListPrice}}
na plataforma VTEX, independentemente de promoções vigentes no site.
Quando há desconto, o preço já entra reduzido diretamente, sem marcar o campo
de desconto. Por isso Pandora aparece com 0\% de SKUs em promoção nesta tabela,
o que não reflete necessariamente ausência de ações promocionais.}}}}

\subsection{{Mix de Material}}

Composição Gold / Silver / Other por bandeira (Consolidated).
Gold = ouro puro ou com componente de ouro. Silver = base prata (inclui banho de ouro).

\begin{{table}}[H]
\centering
\begin{{tabular}}{{lrrr}}
\toprule
\textbf{{Marca}} & \textbf{{Gold}} & \textbf{{Silver}} & \textbf{{Other}} \\
\midrule
{mix_rows}\bottomrule
\end{{tabular}}
\end{{table}}

% =============================================================================
\section{{Seção 2 --- Variação de Preço Efetivo}}
% =============================================================================

Variação calculada sobre \texttt{{preco\_atual}} (preço pago pelo cliente).
\textbf{{Avg.adj}}: média simples por SKU.
\textbf{{Wtd.adj}}: ponderado por \texttt{{preco\_atual}} anterior (mais representativo da receita).
\textcolor{{ForestGreen}}{{Verde}} = alta de preço. \textcolor{{BrickRed}}{{Vermelho}} = queda de preço.
{sem2_aviso}
{secao2_blocks}

\end{{document}}
"""
    return doc


def top_skus_variacao(df_novo: pd.DataFrame, df_ant: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    """
    Retorna os N SKUs com maior variação absoluta de preco_atual por marca.
    Robusto a snapshots que nao tenham nome_produto ou categoria_btg.
    """
    rows = []
    # Colunas opcionais — usa o que estiver disponível
    tem_nome = "nome_produto" in df_novo.columns
    tem_cat  = "categoria_btg" in df_novo.columns

    cols_novo = ["sku_id", "preco_atual"]
    if tem_nome: cols_novo.append("nome_produto")
    if tem_cat:  cols_novo.append("categoria_btg")

    for marca in MARCAS_COM_PANDORA:
        novo_m = df_novo[df_novo["marca"] == marca][cols_novo].copy()
        ant_m  = df_ant[df_ant["marca"] == marca][["sku_id","preco_atual"]].rename(
            columns={"preco_atual": "preco_ant"}
        )
        if len(novo_m) == 0 or len(ant_m) == 0:
            continue
        merged = novo_m.merge(ant_m, on="sku_id", how="inner")
        merged = merged[(merged["preco_ant"] > 0) & (merged["preco_atual"] > 0)].copy()
        merged["var_pct"] = (merged["preco_atual"] - merged["preco_ant"]) / merged["preco_ant"] * 100
        merged["var_abs"] = merged["var_pct"].abs()
        changed = merged[merged["var_abs"] > 0.01].nlargest(n, "var_abs")
        for _, row in changed.iterrows():
            rows.append({
                "Marca":      marca,
                "Produto":    row.get("nome_produto", row["sku_id"]),
                "Categoria":  row.get("categoria_btg", "—"),
                "Preco_Ant":  row["preco_ant"],
                "Preco_Novo": row["preco_atual"],
                "Var_pct":    round(row["var_pct"], 1),
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
        df_top  = top_skus_variacao(df_hoje, df_ant, n=3)
        ajuste_tables.append(df_aj)
        print(f"\n{sep}")
        print(f"AJUSTE POR BANDEIRA — {label}")
        print(sep)
        print(df_band[["Período","Bandeira","N_SKUs","Avg.adj","Wtd.adj"]].to_string(index=False))
        print(f"\n{sep}")
        print(f"AJUSTE CONSOLIDADO + GOLD/SILVER — {label}")
        print(sep)
        print(df_aj[cols_display].to_string(index=False))
        return df_band, df_aj, df_top

    df_band_s1 = df_aj_s1 = df_top_s1 = None
    data_semana1 = find_primeiro_snapshot()
    if data_semana1 is not None and data_semana1 < base_date:
        label_s1 = f"vs. semana 1 ({data_semana1.strftime('%d/%m/%Y')})"
        log.info(f"\nBuscando snapshot semana 1: {data_semana1}")
        df_s1 = load_snapshot_completo(data_semana1, com_pandora=com_pandora)
        if df_s1 is not None:
            log.info(f"  SKUs semana 1: {len(df_s1)}")
            df_band_s1, df_aj_s1, df_top_s1 = rodar_periodo(label_s1, df_s1)
        else:
            log.warning("  Snapshot semana 1 não carregado — pulando")
    else:
        log.warning("  Semana 1 = hoje (só um snapshot disponível) — variação indisponível")

    df_band_mom = df_aj_mom = df_top_mom = None
    data_mom = base_date - timedelta(days=28)
    log.info(f"\nBuscando snapshot MoM: {data_mom}")
    df_mom = load_snapshot_completo(data_mom, com_pandora=com_pandora)
    if df_mom is not None:
        label_mom = f"MoM (vs {data_mom.strftime('%d/%m/%Y')})"
        log.info(f"  SKUs MoM: {len(df_mom)}")
        df_band_mom, df_aj_mom, df_top_mom = rodar_periodo(label_mom, df_mom)
    else:
        log.warning(f"  Snapshot MoM não encontrado — disponível após 28 dias de coleta")

    # ── Export ──
    if args.export:
        # Subpasta por data: data/reports/YYYY-MM-DD/
        # Garante que cada rodada semanal tem sua própria pasta isolada.
        # Criada com parents=True para criar data/reports/ se não existir.
        base_str  = base_date.isoformat()
        day_dir   = REPORTS_DIR / base_str
        day_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"  Pasta de relatórios: {day_dir}")

        # Seção 1 — arquivos sem prefixo de data (a pasta já tem a data)
        df_ticket.to_csv(day_dir / "ticket.csv", index=False)
        df_gift.to_csv(day_dir / "giftability.csv", index=False)
        df_desc.to_csv(day_dir / "descontos.csv", index=False)
        df_mix.to_csv(day_dir / "mix_material.csv", index=False)
        log.info("  Seção 1 exportada (ticket, giftability, descontos, mix_material)")

        # Seção 2 — só exporta os períodos que existem
        # Se não há MoM (< 4 semanas de coleta), periodos_latex fica com só
        # o período vs. semana 1 (ou vazio se for a primeira coleta).
        # O relatório é gerado de qualquer forma — seção 2 fica em branco
        # com um aviso explicativo quando não há períodos disponíveis.
        periodos_latex = []
        for df_band, df_aj, df_top in [
            (df_band_s1, df_aj_s1, df_top_s1),
            (df_band_mom, df_aj_mom, df_top_mom)
        ]:
            if df_aj is None:
                continue
            slug = (df_aj["Período"].iloc[0]
                    .replace(" ","_").replace("(","").replace(")","").replace("/",""))
            df_aj.to_csv(day_dir / f"ajustes_{slug}.csv", index=False)
            df_band.to_csv(day_dir / f"bandeiras_{slug}.csv", index=False)
            if df_top is not None and not df_top.empty:
                df_top.to_csv(day_dir / f"top_skus_{slug}.csv", index=False)
            periodos_latex.append({
                "label":   df_aj["Período"].iloc[0],
                "df_band": df_band,
                "df_aj":   df_aj,
                "df_top":  df_top if df_top is not None else pd.DataFrame(),
            })
        if periodos_latex:
            log.info(f"  Seção 2 exportada ({len(periodos_latex)} período(s))")
        else:
            log.warning("  Seção 2: nenhum período disponível ainda — "
                        "relatório será gerado só com Seção 1")

        # LaTeX + PDF — gerado sempre, independente de ter ou não períodos
        latex_str = gerar_latex(
            base_date=base_date,
            df_hoje=df_hoje,
            df_ticket=df_ticket,
            df_gift=df_gift,
            df_desc=df_desc,
            df_mix=df_mix,
            periodos=periodos_latex,
        )
        latex_path = day_dir / "relatorio.tex"
        with open(latex_path, "w", encoding="utf-8") as f:
            f.write(latex_str)
        log.info(f"  LaTeX gerado: {latex_path}")

        # ── Compilar PDF ──────────────────────────────────────────────────────
        # Duas passagens: 1ª gera estrutura, 2ª resolve índice e referências.
        # cwd = pasta do dia para que os arquivos auxiliares fiquem lá também.
        import subprocess, shutil
        pdflatex = shutil.which("pdflatex")
        if pdflatex is None:
            log.warning("  pdflatex nao encontrado no PATH — PDF nao gerado.")
            log.warning("  Instale MiKTeX (miktex.org) e tente novamente.")
        else:
            pdf_path = day_dir / "relatorio.pdf"
            cmd = [pdflatex, "-interaction=nonstopmode", latex_path.name]
            ok = True
            for rodada in [1, 2]:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    cwd=str(day_dir.resolve()),
                )
                stdout_text = result.stdout.decode("utf-8", errors="replace")
                # pdflatex retorna codigo != 0 mesmo em warnings (nao apenas erros)
                # Verificar se o PDF foi gerado é mais confiável
                pdf_gerado = (day_dir / "relatorio.pdf").exists()
                if not pdf_gerado and rodada == 2:
                    log.error(f"  Erro ao compilar PDF (codigo {result.returncode})")
                    ultimas = "\n".join(stdout_text.splitlines()[-20:])
                    log.error(f"  Ultimas linhas do log:\n{ultimas}")
                    ok = False
                else:
                    log.info(f"  pdflatex passagem {rodada}/2 OK")

            # Limpar arquivos auxiliares
            for ext in [".aux", ".log", ".toc", ".out"]:
                aux = day_dir / f"relatorio{ext}"
                if aux.exists():
                    aux.unlink()

            if ok and pdf_path.exists():
                log.info(f"  PDF gerado: {pdf_path}")
            else:
                log.warning("  PDF nao gerado — veja erros acima ou abra o .tex no Overleaf.")

        log.info(f"\nRelatórios em: {day_dir}")

if __name__ == "__main__":
    main()
