"""
vivara_tracker/scraper_pandora.py
Coleta snapshot de preços por SKU da Pandora Brasil (pandorajoias.com.br).
Mesma plataforma VTEX da Vivara — mesmo formato de output para facilitar o merge.

Uso:
    python scraper_pandora.py              # coleta completa
    python scraper_pandora.py --dry-run    # só primeira página por categoria
    python scraper_pandora.py --cats Rings # só uma categoria

Output: data/snapshots/YYYY-MM-DD_pandora_skus.csv.gz
Colunas idênticas ao snapshot da Vivara para merge direto no analyzer.
"""

import requests
import pandas as pd
import time
import argparse
import logging
from datetime import date
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Configuração ──────────────────────────────────────────────────────────────

BASE_URL  = "https://www.pandorajoias.com.br"
PAGE_SIZE = 50
DELAY_BETWEEN_PAGES = 0.4
DELAY_BETWEEN_CATS  = 1.0

# Diferença chave vs. Vivara:
# - marca fixa "Pandora" (não há sub-marcas)
# - campo de metal é "Metal", não "Material"
# - não há categoria "Correntes" (Chains) separada
CATEGORIES = [
    {"path_vtex": "aneis",     "label_btg": "Rings"},
    {"path_vtex": "braceletes","label_btg": "Bracelets"},
    {"path_vtex": "brincos",   "label_btg": "Earrings"},
    {"path_vtex": "colares",   "label_btg": "Necklaces"},
    {"path_vtex": "charms",    "label_btg": "Pendants/Charms"},
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; equity-research-tracker/1.0)",
}

# ── Normalização de metal → label BTG ────────────────────────────────────────
# Pandora usa campo "Metal" (não "Material" — que é a pedra/zircônia)
# Valores observados: "Prata de Lei", "Revestido a Ouro", "Revestido a Ouro Rosé",
#                     "Dois tons", "Rose"
#
# Regra: base metálica determina o bucket (mesma lógica da Monte Carlo e Vivara)
#   "Prata de Lei"             → Silver (prata pura)
#   "Revestido a Ouro"         → Silver (base prata com revestimento de ouro)
#   "Revestido a Ouro Rosé"    → Silver (base prata com revestimento rosé)
#     Decisão: "Revestido" = banho superficial, a base e o custo de commodity
#     são da prata. Classificar como Gold distorceria o mix de material.
#   "Dois tons"                → Other (bimetálico, não há base dominante clara)
#   "Rose"                     → Other (liga metálica, não é ouro nem prata puro)
def normalizar_metal(metal_raw: str | None) -> str:
    if not metal_raw:
        return "Other"
    m = metal_raw.lower().strip()
    # Prata como base — incluindo revestido/banhado a ouro (base ainda é prata)
    if "prata" in m or "silver" in m or "revestido" in m:
        return "Silver"
    # Ouro puro (sem menção de revestimento ou prata como base)
    if "ouro" in m or "gold" in m:
        return "Gold"
    return "Other"

def faixa_preco(preco: float) -> str:
    if preco <= 300:   return "R$0-300"
    elif preco <= 1000: return "R$300-1000"
    elif preco <= 3000: return "R$1000-3000"
    elif preco <= 10000: return "R$3000-10000"
    else:               return "R$10000+"

# ── Coleta ────────────────────────────────────────────────────────────────────

def get_total(path_vtex: str) -> int:
    url = f"{BASE_URL}/api/catalog_system/pub/products/search/{path_vtex}?_from=0&_to=0"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    resources = r.headers.get("resources", "0-0/0")
    return int(resources.split("/")[1])

def fetch_page(path_vtex: str, from_: int, to_: int) -> list[dict]:
    url = (
        f"{BASE_URL}/api/catalog_system/pub/products/search/{path_vtex}"
        f"?_from={from_}&_to={to_}"
    )
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_produto(produto: dict, label_btg: str) -> list[dict]:
    """
    Converte um produto Pandora (com N SKUs de tamanho) em N linhas.
    Lógica idêntica à Vivara, com ajuste no campo de metal.
    """
    rows = []

    # Campo "Metal" da Pandora = equivalente ao "Material" da Vivara
    metal_raw = produto.get("Metal", [None])[0] if produto.get("Metal") else None
    material_btg = normalizar_metal(metal_raw)

    cat_path = produto.get("categories", [""])[0] if produto.get("categories") else ""

    for sku in produto.get("items", []):
        offer = sku.get("sellers", [{}])[0].get("commertialOffer", {})

        price      = offer.get("Price", 0) or 0
        list_price = offer.get("ListPrice", 0) or 0
        avail_qty  = offer.get("AvailableQuantity", 0) or 0

        # Ignorar SKUs sem preço definido (sem preço = produto inativo)
        # SKUs sem estoque são mantidos na base com disponivel=False
        # O analyzer filtra disponivel==True antes de qualquer análise
        if list_price == 0:
            continue

        em_desconto  = price < list_price and price > 0
        pct_desconto = round((1 - price / list_price) * 100, 2) if em_desconto else 0.0

        rows.append({
            "sku_id":        sku.get("itemId"),
            "product_id":    produto.get("productId"),
            "referencia":    sku.get("referenceId", [{}])[0].get("Value") if sku.get("referenceId") else None,
            "nome_produto":  produto.get("productName"),
            "marca":         "Pandora",
            "categoria_btg": label_btg,
            "categoria_vtex": cat_path,
            "material_raw":  metal_raw,
            "material_btg":  material_btg,
            "preco_atual":   price,
            "preco_tabela":  list_price,
            "em_desconto":   em_desconto,
            "pct_desconto":  pct_desconto,
            "disponivel":    avail_qty > 0,
            "avail_qty":     avail_qty,
            "faixa_preco":   faixa_preco(list_price),
        })
    return rows

def coletar_categoria(cat: dict, dry_run: bool = False) -> list[dict]:
    path  = cat["path_vtex"]
    label = cat["label_btg"]

    total = get_total(path)
    log.info(f"  Pandora / {label}: {total} produtos")

    if dry_run:
        total = min(total, PAGE_SIZE)

    rows  = []
    from_ = 0
    while from_ < total:
        to_     = min(from_ + PAGE_SIZE - 1, total - 1)
        prods   = fetch_page(path, from_, to_)
        for p in prods:
            rows.extend(parse_produto(p, label))
        log.info(f"    [{from_}–{to_}] {len(prods)} produtos → {len(rows)} SKUs acumulados")
        from_ += PAGE_SIZE
        if from_ < total:
            time.sleep(DELAY_BETWEEN_PAGES)

    return rows

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pandora price tracker — coleta snapshot")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cats", nargs="+", metavar="CAT")
    parser.add_argument("--output-dir", default="data/snapshots")
    args = parser.parse_args()

    cats = CATEGORIES
    if args.cats:
        cats = [c for c in CATEGORIES if c["label_btg"] in args.cats]
        log.info(f"Filtrando categorias: {[c['label_btg'] for c in cats]}")

    today    = date.today().isoformat()
    all_rows = []

    log.info(f"=== Pandora — iniciando coleta: {today} ===")
    for cat in cats:
        log.info(f"Coletando: Pandora / {cat['label_btg']} ({cat['path_vtex']})")
        try:
            rows = coletar_categoria(cat, dry_run=args.dry_run)
            all_rows.extend(rows)
        except Exception as e:
            log.error(f"Erro em {cat['path_vtex']}: {e}")
        time.sleep(DELAY_BETWEEN_CATS)

    if not all_rows:
        log.warning("Nenhum dado coletado.")
        return

    df = pd.DataFrame(all_rows)
    df["data_coleta"] = today

    # Deduplicar por sku_id
    antes = len(df)
    df = df.drop_duplicates(subset=["sku_id"])
    if len(df) < antes:
        log.info(f"Removidas {antes - len(df)} duplicatas de SKU")

    log.info(f"\n{'='*50}")
    log.info(f"Total SKUs coletados: {len(df)}")
    log.info(f"Distribuição por categoria:\n{df.groupby('categoria_btg').size().to_string()}")
    log.info(f"Distribuição por metal:\n{df.groupby('material_btg').size().to_string()}")
    log.info(f"SKUs em desconto: {df['em_desconto'].sum()} ({df['em_desconto'].mean()*100:.1f}%)")
    log.info(f"Ticket médio: R$ {df['preco_tabela'].mean():.0f} | Mediana: R$ {df['preco_tabela'].median():.0f}")

    if args.dry_run:
        log.info("[DRY RUN] Não salvando arquivo.")
        print(df[["categoria_btg","material_btg","preco_tabela","preco_atual","em_desconto","disponivel"]].head(20).to_string())
        return

    out_dir  = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today}_pandora_skus.csv.gz"
    df.to_csv(out_path, index=False, compression="gzip")
    log.info(f"Snapshot salvo: {out_path} ({out_path.stat().st_size // 1024} KB)")

if __name__ == "__main__":
    main()
