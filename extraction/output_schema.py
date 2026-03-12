"""
extraction/output_schema.py
────────────────────────────
Single Source of Truth for the Output Column Schema
──────────────────────────────────────────────────────
This is the ONLY file that needs to change when:

  • Adding a new output column
  • Removing an output column
  • Reordering output columns
  • Renaming an output column header

Everything downstream (excel_builder, duplicate_checker, routes, tests)
reads from this module rather than hardcoding column positions.

HOW TO ADD A NEW COLUMN
───────────────────────
1. Add an entry to OUTPUT_COLUMNS below.
2. If the data should come from a detected column in the Excel file,
   add the field name to _FIELD_TO_MAPPER in adaptive_extractor.py.
3. That's it.  No other code changes needed.

HOW TO REMOVE A COLUMN
───────────────────────
1. Comment out or delete the entry from OUTPUT_COLUMNS.
2. Done.  Downstream code rebuilds from this list at import time.

HOW TO RENAME A COLUMN HEADER
───────────────────────────────
1. Change the "col" value in the relevant entry.
2. Done.

COLUMN ENTRY FORMAT
────────────────────
Each entry is a dict with:
    col      (str)   — exact header text written to the output Excel file
    field    (str)   — internal field name used in extraction dicts
    default  (any)   — value when field is absent (None or 0)
    width    (int)   — column width in the output Excel file (characters)
    notes    (str)   — human-readable description (optional, not written to output)
"""

OUTPUT_COLUMNS: list[dict] = [
    # ── Identity ──────────────────────────────────────────────────────────────
    {"col": "SPIR NO",                    "field": "spir_no",         "default": None,  "width": 24,
     "notes": "SPIR document number (e.g. VEN-4460-KAHS-5-43-1002)"},

    {"col": "TAG NO",                     "field": "tag_no",          "default": None,  "width": 22,
     "notes": "Equipment tag number"},

    # ── Equipment metadata ────────────────────────────────────────────────────
    {"col": "EQPT MAKE",                  "field": "manufacturer",    "default": None,  "width": 28,
     "notes": "Equipment manufacturer / make"},

    {"col": "EQPT MODEL",                 "field": "model",           "default": None,  "width": 24,
     "notes": "Equipment model or type"},

    {"col": "EQPT SR NO",                 "field": "serial",          "default": None,  "width": 12,
     "notes": "Equipment serial number"},

    {"col": "EQPT QTY",                   "field": "eqpt_qty",        "default": None,  "width": 10,
     "notes": "Number of units of this equipment installed"},

    # ── Item data ─────────────────────────────────────────────────────────────
    {"col": "QUANTITY IDENTICAL PARTS FITTED", "field": "qty_identical", "default": None, "width": 12,
     "notes": "Total identical spare parts fitted across all units"},

    {"col": "ITEM NUMBER",                "field": "item_num",        "default": None,  "width": 10,
     "notes": "Item sequence number within the SPIR"},

    {"col": "DESCRIPTION OF PARTS",       "field": "desc",            "default": None,  "width": 50,
     "notes": "Original description from the SPIR file"},

    {"col": "NEW DESCRIPTION OF PARTS",   "field": "new_desc",        "default": None,  "width": 60,
     "notes": "Enriched description: Description + Part Number + Supplier (non-placeholder parts)"},

    {"col": "DWG NO INCL POSN NO",        "field": "dwg_no",          "default": None,  "width": 42,
     "notes": "Drawing number including position reference"},

    {"col": "MANUFACTURER PART NUMBER",   "field": "mfr_part_no",     "default": None,  "width": 36,
     "notes": "Manufacturer's part number (preferred over supplier part)"},

    {"col": "SUPPLIER OCM NAME",          "field": "supplier_name",   "default": None,  "width": 28,
     "notes": "Supplier or OCM name"},

    # ── Commercial ────────────────────────────────────────────────────────────
    {"col": "CURRENCY",                   "field": "currency",        "default": None,  "width": 24,
     "notes": "Currency code / description"},

    {"col": "UNIT PRICE",                 "field": "unit_price",      "default": None,  "width": 12,
     "notes": "Unit price"},

    {"col": "DELIVERY TIME IN WEEKS",     "field": "delivery",        "default": None,  "width": 14,
     "notes": "Lead time in weeks"},

    {"col": "MIN MAX STOCK LVLS QTY",     "field": "min_max",         "default": None,  "width": 14,
     "notes": "Min/max stock level quantity"},

    {"col": "UNIT OF MEASURE",            "field": "uom",             "default": None,  "width": 14,
     "notes": "Unit of measure (e.g. PC, SET)"},

    # ── SAP / Classification ──────────────────────────────────────────────────
    {"col": "SAP NUMBER",                 "field": "sap_no",          "default": None,  "width": 16,
     "notes": "SAP material number"},

    {"col": "CLASSIFICATION OF PARTS",    "field": "classification",  "default": None,  "width": 20,
     "notes": "ABC / stock classification code"},

    # ── System columns ────────────────────────────────────────────────────────
    {"col": "DUPLICATE ID",               "field": "duplicate_id",    "default": 0,     "width": 22,
     "notes": "'Duplicate N' or 'SAP NUMBER MISMATCH' — set by duplicate_checker"},

    {"col": "SHEET",                      "field": "sheet",           "default": None,  "width": 22,
     "notes": "Source sheet name"},

    {"col": "SPIR TYPE",                  "field": "spir_type",       "default": None,  "width": 26,
     "notes": "Commissioning / Initial / Normal Operating / Life Cycle Spares"},
]


# ── Derived helpers (do not modify — these are computed from OUTPUT_COLUMNS) ──

#: Flat list of column header strings — used by openpyxl to write the header row
OUTPUT_COLS: list[str] = [e["col"] for e in OUTPUT_COLUMNS]

#: Column index map: col_name → 0-based index into OUTPUT_COLS
CI: dict[str, int] = {e["col"]: i for i, e in enumerate(OUTPUT_COLUMNS)}

#: Column widths map: col_name → width
COL_WIDTHS: dict[str, int] = {e["col"]: e["width"] for e in OUTPUT_COLUMNS}

#: Field→column index map: internal_field → 0-based index
FIELD_CI: dict[str, int] = {e["field"]: i for i, e in enumerate(OUTPUT_COLUMNS)}


def make_empty_row() -> list:
    """Return a new OUTPUT_COLS-length list with all defaults."""
    return [e["default"] for e in OUTPUT_COLUMNS]


def row_from_dict(item: dict) -> list:
    """
    Convert an internal item dict to a OUTPUT_COLS-ordered list.

    Any field not in item falls back to the schema default.
    This is the canonical way to turn extracted data into output rows.

    Example:
        row = row_from_dict({
            'spir_no': 'VEN-001',
            'tag_no':  '10-P-1001',
            'desc':    'Gasket NPS 6',
            'sap_no':  '100001',
        })
        # → list of 23 values, all others are None/0
    """
    row = make_empty_row()
    for entry in OUTPUT_COLUMNS:
        col_idx = CI[entry["col"]]
        row[col_idx] = item.get(entry["field"], entry["default"])
    return row
