"""
vivara_tracker/scraper_montecarlo.py
Coleta snapshot de preços por SKU da Monte Carlo (montecarlo.com.br).
Mesma plataforma VTEX — mesmo formato de output para merge direto no analyzer.

Estrutura de marcas (análogo à Vivara):
  Monte Carlo  ↔  Vivara   (marca premium, joias em ouro e prata)
  Jolie        ↔  Life     (sub-marca, ticket mais baixo, mais prata)

Uso:
    python scraper_montecarlo.py              # coleta completa (~15 min)
    python scraper_montecarlo.py --dry-run    # só primeira página por categoria
    python scraper_montecarlo.py --cats Rings # só uma categoria

Output: data/snapshots/YYYY-MM-DD_montecarlo_skus.csv.gz
"""

import requests
import pandas as pd
import time
import argparse
import logging
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BASE_URL  = "https://www.montecarlo.com.br"
PAGE_SIZE = 50
DELAY_BETWEEN_PAGES = 0.4
DELAY_BETWEEN_CATS  = 1.0

# Categorias Monte Carlo (principal) + Jolie (sub-marca, análoga ao Life da Vivara)
# Sub-categorias (ex: joias/aneis/solitarios) já incluídas no pai — sem duplicatas.
# Piercings (30 SKUs) e Tornozeleiras (4 SKUs) excluídos: volume irrisório,
# sem equivalente nas outras marcas monitoradas.
CATEGORIES = [
    # Monte Carlo
    {"marca": "MonteCarlo", "path_vtex": "joias/aneis",     "label_btg": "Rings"},
    {"marca": "MonteCarlo", "path_vtex": "joias/brincos",   "label_btg": "Earrings"},
    {"marca": "MonteCarlo", "path_vtex": "joias/colares",   "label_btg": "Necklaces"},
    {"marca": "MonteCarlo", "path_vtex": "joias/correntes", "label_btg": "Chains"},
    {"marca": "MonteCarlo", "path_vtex": "joias/pulseiras", "label_btg": "Bracelets"},
    {"marca": "MonteCarlo", "path_vtex": "joias/pingentes", "label_btg": "Pendants/Charms"},
    # Jolie by Monte Carlo
    {"marca": "Jolie",      "path_vtex": "jolie/aneis",     "label_btg": "Rings"},
    {"marca": "Jolie",      "path_vtex": "jolie/brincos",   "label_btg": "Earrings"},
    {"marca": "Jolie",      "path_vtex": "jolie/colares",   "label_btg": "Necklaces"},
    {"marca": "Jolie",      "path_vtex": "jolie/pulseiras", "label_btg": "Bracelets"},
    {"marca": "Jolie",      "path_vtex": "jolie/charms",    "label_btg": "Pendants/Charms"},
    {"marca": "Jolie",      "path_vtex": "jolie/pingentes", "label_btg": "Pendants/Charms"},
]
# Jolie nao tem Chains como categoria separada.

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; equity-research-tracker/1.0)",
}

def normalizar_material(mat_raw: str | None) -> str:
    """
    Campo Material da Monte Carlo: "Ouro Amarelo", "Prata", "Prata com Banho de Ouro Amarelo", etc.
    Regra: base metalica determina o bucket.
      Ouro puro (sem prata como base) → Gold
      Prata pura ou prata com banho de ouro → Silver
        (custo e sensibilidade a commodity seguem a prata, nao o ouro do banho)
      Aco, sem material → Other
    """
    if not mat_raw:
        return "Other"
    m = mat_raw.lower().strip()
    if ("ouro" in m or "gold" in m) and "prata" not in m:
        return "Gold"
    if "prata" in m or "silver" in m:
        return "Silver"
    return "Other"

def faixa_preco(preco: float) -> str:
    if preco <= 300:     return "R$0-300"
    elif preco <= 1000:  return "R$300-1000"
    elif preco <= 3000:  return "R$1000-3000"
    elif preco <= 10000: return "R$3000-10000"
    else:                return "R$10000+"

def get_total(path_vtex: str) -> int:
    url = f"{BASE_URL}/api/catalog_system/pub/products/search/{path_vtex}?_from=0&_to=0"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return int((r.headers.get("resources", "0-0/0")).split("/")[1])

def fetch_page(path_vtex: str, from_: int, to_: int) -> list[dict]:
    url = f"{BASE_URL}/api/catalog_system/pub/products/search/{path_vtex}?_from={from_}&_to={to_}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_produto(produto: dict, marca: str, label_btg: str) -> list[dict]:
    rows = []
    mat_raw      = produto.get("Material", [None])[0] if produto.get("Material") else None
    material_btg = normalizar_material(mat_raw)
    cat_path     = produto.get("categories", [""])[0] if produto.get("categories") else ""

    for sku in produto.get("items", []):
        offer      = sku.get("sellers", [{}])[0].get("commertialOffer", {})
        price      = offer.get("Price", 0) or 0
        list_price = offer.get("ListPrice", 0) or 0
        avail_qty  = offer.get("AvailableQuantity", 0) or 0

        if list_price == 0:
            continue

        em_desconto  = price < list_price and price > 0
        pct_desconto = round((1 - price / list_price) * 100, 2) if em_desconto else 0.0

        rows.append({
            "sku_id":         sku.get("itemId"),
            "product_id":     produto.get("productId"),
            "referencia":     sku.get("referenceId", [{}])[0].get("Value") if sku.get("referenceId") else None,
            "nome_produto":   produto.get("productName"),
            "marca":          marca,
            "categoria_btg":  label_btg,
            "categoria_vtex": cat_path,
            "material_raw":   mat_raw,
            "material_btg":   material_btg,
            "preco_atual":    price,
            "preco_tabela":   list_price,
            "em_desconto":    em_desconto,
            "pct_desconto":   pct_desconto,
            "disponivel":     avail_qty > 0,
            "avail_qty":      avail_qty,
            "faixa_preco":    faixa_preco(list_price),
        })
    return rows

def coletar_categoria(cat: dict, dry_run: bool = False) -> list[dict]:
    path  = cat["path_vtex"]
    marca = cat["marca"]
    label = cat["label_btg"]
    total = get_total(path)
    log.info(f"  {marca} / {label}: {total} produtos")
    if dry_run:
        total = min(total, PAGE_SIZE)

    rows  = []
    from_ = 0
    while from_ < total:
        to_   = min(from_ + PAGE_SIZE - 1, total - 1)
        prods = fetch_page(path, from_, to_)
        for p in prods:
            rows.extend(parse_produto(p, marca, label))
        log.info(f"    [{from_}-{to_}] {len(prods)} produtos -> {len(rows)} SKUs acumulados")
        from_ += PAGE_SIZE
        if from_ < total:
            time.sleep(DELAY_BETWEEN_PAGES)
    return rows

def main():
    parser = argparse.ArgumentParser(description="Monte Carlo price tracker")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cats", nargs="+", metavar="CAT")
    parser.add_argument("--marca", choices=["MonteCarlo", "Jolie"])
    parser.add_argument("--output-dir", default="data/snapshots")
    args = parser.parse_args()

    cats = CATEGORIES
    if args.cats:
        cats = [c for c in cats if c["label_btg"] in args.cats]
    if args.marca:
        cats = [c for c in cats if c["marca"] == args.marca]

    today    = date.today().isoformat()
    all_rows = []

    log.info(f"=== Monte Carlo + Jolie — coleta: {today} ===")
    for cat in cats:
        log.info(f"Coletando: {cat['marca']} / {cat['label_btg']} ({cat['path_vtex']})")
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

    # Deduplicar por sku_id (protege contra overlap entre charms e pingentes da Jolie)
    antes = len(df)
    df = df.drop_duplicates(subset=["sku_id"])
    if len(df) < antes:
        log.info(f"Removidas {antes - len(df)} duplicatas de SKU")

    log.info(f"\n{'='*50}")
    log.info(f"Total SKUs: {len(df)} | Disponíveis: {df['disponivel'].sum()} | Sem estoque: {(~df['disponivel']).sum()}")
    log.info(f"Por marca:\n{df.groupby('marca').size().to_string()}")
    log.info(f"Por categoria:\n{df.groupby(['marca','categoria_btg']).size().to_string()}")
    log.info(f"Por material:\n{df.groupby(['marca','material_btg']).size().to_string()}")
    log.info(f"SKUs em desconto: {df['em_desconto'].sum()} ({df['em_desconto'].mean()*100:.1f}%)")
    disp = df[df['disponivel']]
    mc_ticket = disp[disp['marca']=='MonteCarlo']['preco_tabela']
    jo_ticket = disp[disp['marca']=='Jolie']['preco_tabela']
    if len(mc_ticket): log.info(f"Ticket medio MonteCarlo: R${mc_ticket.mean():.0f} | Mediana: R${mc_ticket.median():.0f}")
    if len(jo_ticket): log.info(f"Ticket medio Jolie:      R${jo_ticket.mean():.0f} | Mediana: R${jo_ticket.median():.0f}")

    if args.dry_run:
        log.info("[DRY RUN] Nao salvando arquivo.")
        print(df[["marca","categoria_btg","material_btg","preco_tabela","preco_atual","em_desconto","disponivel"]].head(20).to_string())
        return

    out_dir  = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today}_montecarlo_skus.csv.gz"
    df.to_csv(out_path, index=False, compression="gzip")
    log.info(f"Snapshot salvo: {out_path} ({out_path.stat().st_size // 1024} KB)")

if __name__ == "__main__":
    main()
