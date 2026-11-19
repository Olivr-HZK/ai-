"""Frozen, explicit product aliases; never infer cross-store identity by similarity."""
import html
import json
from pathlib import Path


def normalize(value):
    return ' '.join(html.unescape(str(value or '')).split())


def load_default_mapping():
    path = Path(__file__).resolve().parents[1] / 'config/afizzy_report_products.json'
    raw = json.loads(path.read_text())
    return {'version': 'report-product-aliases-v1', 'products': raw['products']}


def product_for(app, app_id, mapping):
    if mapping.get('version') != 'report-product-aliases-v1':
        raise ValueError('Unsupported product identity mapping')
    aliases = {}
    for product in mapping.get('products', []):
        name = normalize(product.get('name'))
        if not name:
            raise ValueError('Product identity missing name')
        for alias in [name, *product.get('aliases', [])]:
            alias = normalize(alias)
            if not alias:
                raise ValueError('Empty product alias')
            if alias in aliases and aliases[alias] != name:
                raise ValueError('Ambiguous product alias')
            aliases[alias] = name
    by_name, by_id = aliases.get(normalize(app)), aliases.get(normalize(app_id)) if app_id else None
    if by_name and by_id and by_name != by_id:
        raise ValueError('App name and ID conflict')
    if app_id and not by_id:
        return None
    return by_id or by_name
