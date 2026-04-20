"""
vivara_tracker/scraper.py
Coleta snapshot de preços por SKU da Vivara e Life via API VTEX.
Salva em parquet com data da coleta no nome do arquivo.

Uso:
    python scraper.py                  # coleta hoje
    python scraper.py --dry-run        # testa sem salvar
    python scraper.py --cats aneis     # só uma categoria
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

# ── Configuração ─────────────────────────────────────────────────────────────

BASE_URL = "https://www.vivara.com.br"
PAGE_SIZE = 50
DELAY_BETWEEN_PAGES = 0.4   # segundos entre requests (educado com o servidor)
DELAY_BETWEEN_CATS  = 1.0   # entre categorias

# Mapeamento categoria VTEX → label BTG
# path_vtex: caminho usado na API
# label_btg: nome igual ao relatório BTG para facilitar comparação
CATEGORIES = [
    {"marca": "Vivara", "path_vtex": "vivara/joias/aneis",     "label_btg": "Rings"},
    {"marca": "Vivara", "path_vtex": "vivara/joias/brincos",   "label_btg": "Earrings"},
    {"marca": "Vivara", "path_vtex": "vivara/joias/colares",   "label_btg": "Necklaces"},
    {"marca": "Vivara", "path_vtex": "vivara/joias/correntes", "label_btg": "Chains"},
    {"marca": "Vivara", "path_vtex": "vivara/joias/pulseiras", "label_btg": "Bracelets"},
    {"marca": "Vivara", "path_vtex": "vivara/joias/pingentes", "label_btg": "Pendants/Charms"},
    {"marca": "Life",   "path_vtex": "life/joias/aneis",       "label_btg": "Rings"},
    {"marca": "Life",   "path_vtex": "life/joias/brincos",     "label_btg": "Earrings"},
    {"marca": "Life",   "path_vtex": "life/joias/colares",     "label_btg": "Necklaces"},
    {"marca": "Life",   "path_vtex": "life/joias/correntes",   "label_btg": "Chains"},
    {"marca": "Life",   "path_vtex": "life/joias/pulseiras",   "label_btg": "Bracelets"},
    {"marca": "Life",   "path_vtex": "life/joias/pingentes",   "label_btg": "Pendants/Charms"},
]

# Normalização de material → label BTG
# Valores observados na Vivara/Life:
#   "Ouro amarelo", "ouro branco", "Ouro rosé"         → Gold
#   "Prata e Ouro"                                      → Gold (presença de ouro = Gold)
#   "Prata", "Prata + Banho de Ródio Negro"             → Silver (base prata pura)
#   "Prata + Banho de Ouro Amarelo"                    → Silver (banho superficial, base prata)
#   "Liga Rosé + Banho de Ouro Rosé", "Aço Dourado"    → Other
#   "Aço", "Couro"                                      → Other
def normalizar_material(mat_raw: str | None) -> str:
    if not mat_raw:
        return "Other"
    m = mat_raw.lower().strip()
    # Couro deve vir antes de ouro — "couro" contém "ouro" como substring
    if "couro" in m:
        return "Leather"
    # Liga Rosé = liga metálica própria → Other (antes de checar ouro)
    if m.startswith("liga"):
        return "Other"
    # "Prata e Ouro" → Gold (presença de ouro como componente = Gold)
    if "prata e ouro" in m:
        return "Gold"
    # Prata como base sem ouro como componente → Silver
    if "prata" in m and "ouro" not in m:
        return "Silver"
    # "Prata + Banho de Ouro" → Silver (banho superficial, base é prata)
    if "prata" in m and "banho" in m:
        return "Silver"
    # Ouro (puro ou como componente dominante)
    if "ouro" in m or "gold" in m:
        return "Gold"
    return "Other"

def faixa_preco(preco: float) -> str:
    if preco <= 300:
        return "R$0-300"
    elif preco <= 1000:
        return "R$300-1000"
    elif preco <= 3000:
        return "R$1000-3000"
    elif preco <= 10000:
        return "R$3000-10000"
    else:
        return "R$10000+"

# ── Funções de coleta ─────────────────────────────────────────────────────────

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; equity-research-tracker/1.0)",
}

def get_total(path_vtex: str) -> int:
    """Retorna o total de produtos disponíveis via header 'resources'."""
    url = f"{BASE_URL}/api/catalog_system/pub/products/search/{path_vtex}?_from=0&_to=0"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    resources = r.headers.get("resources", "0-0/0")
    return int(resources.split("/")[1])

def fetch_page(path_vtex: str, from_: int, to_: int) -> list[dict]:
    """Busca uma página de produtos."""
    url = (
        f"{BASE_URL}/api/catalog_system/pub/products/search/{path_vtex}"
        f"?_from={from_}&_to={to_}"
    )
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_produto(produto: dict, marca: str, label_btg: str) -> list[dict]:
    """
    Converte um produto VTEX (com N SKUs) em N linhas, uma por SKU.
    Retorna lista de dicts prontos para o DataFrame.
    """
    rows = []

    # Material do produto (nível produto, não SKU)
    mat_raw = produto.get("Material", [None])[0] if produto.get("Material") else None
    material_btg = normalizar_material(mat_raw)

    # Categoria mais específica (primeiro path = mais profundo)
    cat_path = produto.get("categories", [""])[0] if produto.get("categories") else ""

    for sku in produto.get("items", []):
        offer = sku.get("sellers", [{}])[0].get("commertialOffer", {})

        price     = offer.get("Price", 0) or 0
        list_price = offer.get("ListPrice", 0) or 0
        avail_qty  = offer.get("AvailableQuantity", 0) or 0

        # Ignorar SKUs sem preço definido (sem preço = produto inativo)
        # SKUs sem estoque são mantidos na base com disponivel=False
        # O analyzer filtra disponivel==True antes de qualquer análise
        if list_price == 0:
            continue

        em_desconto = price < list_price and price > 0
        pct_desconto = round((1 - price / list_price) * 100, 2) if em_desconto else 0.0

        rows.append({
            # IDs
            "sku_id":       sku.get("itemId"),
            "product_id":   produto.get("productId"),
            "referencia":   sku.get("referenceId", [{}])[0].get("Value") if sku.get("referenceId") else None,
            # Descritivos
            "nome_produto": produto.get("productName"),
            "marca":        marca,
            "categoria_btg": label_btg,
            "categoria_vtex": cat_path,
            "material_raw": mat_raw,
            "material_btg": material_btg,
            # Preços
            "preco_atual":    price,
            "preco_tabela":   list_price,
            "em_desconto":    em_desconto,
            "pct_desconto":   pct_desconto,
            # Disponibilidade
            "disponivel":     avail_qty > 0,
            "avail_qty":      avail_qty,
            # Faixa (calculada sobre preço de tabela, sem desconto — como faz o BTG)
            "faixa_preco":    faixa_preco(list_price),
        })
    return rows

def coletar_categoria(cat: dict, dry_run: bool = False) -> list[dict]:
    """Coleta todos os SKUs de uma categoria com paginação."""
    path  = cat["path_vtex"]
    marca = cat["marca"]
    label = cat["label_btg"]

    total = get_total(path)
    log.info(f"  {marca} / {label}: {total} produtos")

    if dry_run:
        total = min(total, PAGE_SIZE)  # só primeira página no dry-run

    rows = []
    from_ = 0
    while from_ < total:
        to_ = min(from_ + PAGE_SIZE - 1, total - 1)
        produtos = fetch_page(path, from_, to_)
        for p in produtos:
            rows.extend(parse_produto(p, marca, label))
        log.info(f"    [{from_}–{to_}] {len(produtos)} produtos → {len(rows)} SKUs acumulados")
        from_ += PAGE_SIZE
        if from_ < total:
            time.sleep(DELAY_BETWEEN_PAGES)

    return rows

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Vivara price tracker — coleta snapshot")
    parser.add_argument("--dry-run", action="store_true",
                        help="Apenas primeira página por categoria, sem salvar")
    parser.add_argument("--cats", nargs="+", metavar="CAT",
                        help="Filtrar por label_btg (ex: Rings Earrings)")
    parser.add_argument("--output-dir", default="data/snapshots",
                        help="Diretório de saída (padrão: data/snapshots)")
    args = parser.parse_args()

    cats = CATEGORIES
    if args.cats:
        cats = [c for c in CATEGORIES if c["label_btg"] in args.cats]
        log.info(f"Filtrando categorias: {[c['label_btg'] for c in cats]}")

    today = date.today().isoformat()  # "2026-04-20"
    all_rows = []

    log.info(f"=== Iniciando coleta: {today} ===")
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

    # Remover duplicatas de sku_id dentro da mesma coleta
    # (pode acontecer se um SKU aparecer em duas categorias pai/filho)
    antes = len(df)
    df = df.drop_duplicates(subset=["sku_id"])
    if len(df) < antes:
        log.info(f"Removidas {antes - len(df)} duplicatas de SKU")

    log.info(f"\n{'='*50}")
    log.info(f"Total SKUs coletados: {len(df)}")
    log.info(f"Distribuição por marca:\n{df.groupby('marca').size().to_string()}")
    log.info(f"Distribuição por categoria:\n{df.groupby(['marca','categoria_btg']).size().to_string()}")
    log.info(f"SKUs em desconto: {df['em_desconto'].sum()} ({df['em_desconto'].mean()*100:.1f}%)")

    if args.dry_run:
        log.info("[DRY RUN] Não salvando arquivo.")
        print(df[["marca","categoria_btg","material_btg","preco_tabela","preco_atual","em_desconto","pct_desconto","disponivel"]].head(20).to_string())
        return

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today}_vivara_skus.csv.gz"
    df.to_csv(out_path, index=False, compression="gzip")
    log.info(f"Snapshot salvo: {out_path} ({out_path.stat().st_size // 1024} KB)")

if __name__ == "__main__":
    main()
