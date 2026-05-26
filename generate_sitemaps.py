#!/usr/bin/env python3
import os, argparse, datetime
import pandas as pd
from urllib.parse import urlparse, urlunparse
from xml.sax.saxutils import escape

PRODUCT_DETAIL_PREFIXES = (
    "/pages/lab-grown-diamonds/",
    "/pages/natural-diamonds/",
    "/pages/gemstones-diamonds/",
)
DEFAULT_PUBLIC_BASE_URL = "https://www.leeladiamond.com/sitemaps"

def normalize_url(u: str) -> str:
    """
    Ensure URLs use https://www.leeladiamond.com and strip fragments.
    Product detail URLs should match the canonical path used by the site.
    """
    u = (u or "").strip()
    if not u:
        return u
    p = urlparse(u)
    if p.netloc.endswith("leeladiamond.com"):
        p = p._replace(scheme="https", netloc="www.leeladiamond.com")
    if p.path.startswith(PRODUCT_DETAIL_PREFIXES):
        p = p._replace(query="")
    return urlunparse(p._replace(fragment=""))


def path_matches(url, prefixes):
    if not prefixes:
        return True
    return urlparse(url).path.startswith(tuple(prefixes))


def iter_links(csv_source, link_col="link", chunksize=200000, include_prefixes=None, max_urls=None):
    # Stream read extremely large CSVs
    yielded = 0
    for chunk in pd.read_csv(csv_source, dtype=str, usecols=[link_col], chunksize=chunksize):
        # preserve as-is (after normalization)
        for url in chunk[link_col].dropna().astype(str):
            url = normalize_url(url)
            if not url or not path_matches(url, include_prefixes):
                continue
            yield url
            yielded += 1
            if max_urls and yielded >= max_urls:
                return

def write_urlset_xml(file_path, urls):
    # Write sitemap with lastmod, priority, and changefreq for better crawl guidance
    today = datetime.datetime.now(datetime.UTC).date().isoformat()
    with open(file_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for u in urls:
            f.write("  <url>\n")
            f.write(f"    <loc>{escape(u)}</loc>\n")
            f.write(f"    <lastmod>{today}</lastmod>\n")
            f.write("    <changefreq>weekly</changefreq>\n")
            f.write("    <priority>0.8</priority>\n")
            f.write("  </url>\n")
        f.write("</urlset>\n")

def write_index_xml(index_path, part_files, public_base_url):
    now = datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for name in part_files:
            loc = f"{public_base_url.rstrip('/')}/{name}"
            f.write("  <sitemap>\n")
            f.write(f"    <loc>{escape(loc)}</loc>\n")
            f.write(f"    <lastmod>{now}</lastmod>\n")
            f.write("  </sitemap>\n")
        f.write("</sitemapindex>\n")


def normalize_public_base_url(value: str, allow_external_host: bool = False) -> str:
    base = (value or DEFAULT_PUBLIC_BASE_URL).strip().rstrip("/")
    parsed = urlparse(base)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("--public-base-url must be an absolute https URL")

    if parsed.scheme != "https":
        raise ValueError("--public-base-url must use https")

    if parsed.netloc in {"leeladiamond.com", "www.leeladiamond.com"}:
        parsed = parsed._replace(netloc="www.leeladiamond.com")
        return urlunparse(parsed)

    if not allow_external_host:
        raise ValueError(
            "--public-base-url must be on www.leeladiamond.com. "
            "Use --allow-external-sitemap-host only if that host is verified in Search Console."
        )

    return base

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="CSV URL or path")
    p.add_argument("--outdir", required=True, help="Output directory")
    p.add_argument("--basename", default="leela-products-", help="Base name for part files")
    p.add_argument("--per-file", type=int, default=45000, help="URLs per sitemap file (must be <= 50k)")
    p.add_argument("--public-base-url", default=DEFAULT_PUBLIC_BASE_URL, help="Base URL where sitemaps are hosted")
    p.add_argument("--index-name", default="sitemap-index.xml", help="Sitemap index filename")
    p.add_argument("--link-column", default="link", help="CSV column containing URLs")
    p.add_argument("--include-prefix", action="append", default=[], help="Only include URLs whose path starts with this prefix. Can be repeated.")
    p.add_argument("--product-details-only", action="store_true", help="Only include canonical diamond/gemstone detail page URLs")
    p.add_argument("--max-urls", type=int, default=0, help="Stop after this many unique URLs. 0 means no cap.")
    p.add_argument("--allow-external-sitemap-host", action="store_true", help="Allow sitemap index child URLs to use a host other than www.leeladiamond.com")
    args = p.parse_args()

    if args.per_file > 50000:
        raise ValueError("--per-file must be 50000 or lower")

    public_base_url = normalize_public_base_url(
        args.public_base_url,
        allow_external_host=args.allow_external_sitemap_host,
    )

    os.makedirs(args.outdir, exist_ok=True)

    buffer, part_names = [], []
    part = 1

    seen = set()
    include_prefixes = list(args.include_prefix)
    if args.product_details_only:
        include_prefixes.extend(PRODUCT_DETAIL_PREFIXES)

    for url in iter_links(
        args.csv,
        args.link_column,
        include_prefixes=include_prefixes,
        max_urls=args.max_urls or None,
    ):
        if url in seen:
            continue
        seen.add(url)

        buffer.append(url)
        if len(buffer) >= args.per_file:
            part_name = f"{args.basename}{part:05d}.xml"
            write_urlset_xml(os.path.join(args.outdir, part_name), buffer)
            part_names.append(part_name)
            buffer.clear()
            part += 1

    if buffer:
        part_name = f"{args.basename}{part:05d}.xml"
        write_urlset_xml(os.path.join(args.outdir, part_name), buffer)
        part_names.append(part_name)

    index_path = os.path.join(args.outdir, args.index_name)
    write_index_xml(index_path, part_names, public_base_url)

    print(f"Generated {len(part_names)} sitemap part files; index at {index_path}")
    print(f"Sitemap child URLs use: {public_base_url}")

if __name__ == "__main__":
    main()
