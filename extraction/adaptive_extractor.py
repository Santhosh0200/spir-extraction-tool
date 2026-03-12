"""
extraction/adaptive_extractor.py
──────────────────────────────────
Adaptive SPIR Extractor
────────────────────────
This is the future-proof extraction layer.

Architecture
────────────
                     ┌─────────────────────────────────┐
  Any SPIR file ───► │  spir_dispatcher  (entry point) │
                     └──────────────┬──────────────────┘
                                    │
                  ┌─────────────────┴──────────────────┐
                  │                                     │
           Known format?                          Unknown format?
         (FORMAT1-FORMAT5)                   (never seen before)
                  │                                     │
                  ▼                                     ▼
          spir_engine.py                   AdaptiveExtractor
          (battle-tested,                   ├── classify_workbook()
           exact results)                   ├── detect_header_row()
                                            ├── map_sheet()
                                            └── _extract_sheet_rows()

The dispatcher tries the known engine first.  If that returns FORMAT-level
confidence but the result has zero rows, or if the workbook does not match
any known format pattern, it falls through to AdaptiveExtractor.

AdaptiveExtractor algorithm (per sheet):
  1.  SheetClassifier determines role (MAIN / CONTINUATION / ANNEXURE)
  2.  HeaderDetector finds the header row
  3.  ColumnMapper maps headers → logical field names
  4.  _extract_sheet_rows() reads every data row below the header
  5.  Rows from all sheets are merged into a single canonical list
  6.  DuplicateDetector flags duplicates (reuses existing engine logic)
  7.  OutputBuilder converts to OUTPUT_COLS format

This produces the same output structure as the known engine, so downstream
services (excel_builder, duplicate_checker) work unchanged.

Adding new SPIR formats in the future
──────────────────────────────────────
In most cases: NOTHING needs to change.  The adaptive extractor will
automatically detect the header row, map columns, and extract data.

Only if a new format has genuinely novel structural patterns (e.g. a
completely new link-back mechanism) does new code need to be written —
and even then, only a single new RowLinker needs to be added.

Public API
──────────
    spir_dispatcher(wb) → dict          # main entry point (replaces extract_spir)
    AdaptiveExtractor(wb).extract() → dict
"""
from __future__ import annotations
import logging
import re
from collections import defaultdict
from typing import Any

from extraction.spir_engine import (
    extract_spir as _engine_extract,
    detect_format as _engine_detect_format,
    OUTPUT_COLS, CI,
    make_new_desc, compute_duplicate_ids, norm,
)
from extraction.header_detector import detect_header_row
from extraction.column_mapper import map_sheet, FIELD_KEYWORDS
from extraction.sheet_classifier import (
    classify_workbook, get_extraction_plan,
    SheetRole, SheetProfile,
)

log = logging.getLogger(__name__)


# ── Output schema (single place to change columns) ────────────────────────────
#
# This maps OUTPUT_COLS field names to AdaptiveExtractor internal field names.
# If you add a new output column, add it here + in OUTPUT_COLS.
# The adaptive extractor will automatically try to fill it from any sheet.

OUTPUT_SCHEMA: list[dict] = [
    # output_col              internal_field       default
    {"col": "SPIR NO",                    "field": "spir_no",         "default": None},
    {"col": "TAG NO",                     "field": "tag_no",          "default": None},
    {"col": "EQPT MAKE",                  "field": "manufacturer",    "default": None},
    {"col": "EQPT MODEL",                 "field": "model",           "default": None},
    {"col": "EQPT SR NO",                 "field": "serial",          "default": None},
    {"col": "EQPT QTY",                   "field": "eqpt_qty",        "default": None},
    {"col": "QUANTITY IDENTICAL PARTS FITTED", "field": "qty_identical", "default": None},
    {"col": "ITEM NUMBER",                "field": "item_num",        "default": None},
    {"col": "DESCRIPTION OF PARTS",       "field": "desc",            "default": None},
    {"col": "NEW DESCRIPTION OF PARTS",   "field": "new_desc",        "default": None},
    {"col": "DWG NO INCL POSN NO",        "field": "dwg_no",          "default": None},
    {"col": "MANUFACTURER PART NUMBER",   "field": "mfr_part_no",     "default": None},
    {"col": "SUPPLIER OCM NAME",          "field": "supplier_name",   "default": None},
    {"col": "CURRENCY",                   "field": "currency",        "default": None},
    {"col": "UNIT PRICE",                 "field": "unit_price",      "default": None},
    {"col": "DELIVERY TIME IN WEEKS",     "field": "delivery",        "default": None},
    {"col": "MIN MAX STOCK LVLS QTY",     "field": "min_max",         "default": None},
    {"col": "UNIT OF MEASURE",            "field": "uom",             "default": None},
    {"col": "SAP NUMBER",                 "field": "sap_no",          "default": None},
    {"col": "CLASSIFICATION OF PARTS",    "field": "classification",  "default": None},
    {"col": "DUPLICATE ID",               "field": "duplicate_id",    "default": 0},
    {"col": "SHEET",                      "field": "sheet",           "default": None},
    {"col": "SPIR TYPE",                  "field": "spir_type",       "default": None},
]

# Map internal field name → ColumnMapper logical field name
# (links OUTPUT_SCHEMA to what ColumnMapper can detect)
_FIELD_TO_MAPPER: dict[str, str] = {
    "item_num":        "ITEM_NUMBER",
    "desc":            "DESCRIPTION",
    "dwg_no":          "DWG_NO",
    "mfr_part_no":     "MFR_PART_NO",
    "supplier_name":   "SUPPLIER_NAME",
    "currency":        "CURRENCY",
    "unit_price":      "UNIT_PRICE",
    "delivery":        "DELIVERY",
    "min_max":         "MIN_MAX",
    "uom":             "UOM",
    "sap_no":          "SAP_NUMBER",
    "classification":  "CLASSIFICATION",
    "qty_identical":   "QTY_IDENTICAL",
    "eqpt_qty":        "NO_OF_UNITS",
    "model":           "MFR_MODEL",
    "serial":          "MFR_SERIAL",
}

# Placeholder strings treated as "no data"
_PLACEHOLDERS = frozenset({
    'tba', 'na', 'n/a', 'n.a', 'n.a.', 'tbc', '-', '.', 'nil',
    'none', 'not applicable', 'not available', 'unknown', '—',
})


# ── Cell reading helpers ───────────────────────────────────────────────────────

def _cv(ws, r: int, c: int) -> str:
    v = ws.cell(r, c).value
    return str(v).strip() if v is not None else ''


def _cn(ws, r: int, c: int):
    v = ws.cell(r, c).value
    if v is None:
        return None
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except (ValueError, TypeError):
        return None


def _real(val) -> bool:
    """Return True if val is non-empty and not a placeholder."""
    if val is None:
        return False
    s = str(val).strip()
    return bool(s) and s.lower() not in _PLACEHOLDERS


def _is_footer_row(desc: str) -> bool:
    """Return True if the description suggests a footer / non-data row."""
    dl = (desc or '').lower().strip()
    return dl.startswith((
        'project', 'company', 'engineering', 'reminder', 'technical data',
        'note:', 'notes:', 'end of', '* ',
    ))


# ── AdaptiveExtractor ─────────────────────────────────────────────────────────

class AdaptiveExtractor:
    """
    Format-agnostic SPIR extractor.

    Processes any workbook by:
    1. Classifying all sheets
    2. Building an extraction plan
    3. Extracting rows from each data sheet using dynamic column mapping
    4. Merging and deduplicating
    """

    def __init__(self, wb):
        self.wb = wb
        self._profiles: dict[str, SheetProfile] = {}
        self._plan = None
        self._global_context: dict[str, Any] = {}   # spir_no, manufacturer, etc.

    def extract(self) -> dict:
        """
        Run the full adaptive extraction pipeline.

        Returns the same dict structure as extract_spir() in spir_engine.py.
        """
        # 1. Classify all sheets
        self._profiles = classify_workbook(self.wb)
        self._plan     = get_extraction_plan(self._profiles)

        if not self._plan.has_data:
            log.warning("No data sheets found in workbook")
            return self._empty_result()

        # 2. Extract global metadata (SPIR no, manufacturer, etc.)
        self._extract_global_context()

        # 3. Extract rows from each sheet type
        raw_items: list[dict] = []
        annexure_stats: dict[str, int] = {}

        # Main sheets
        for profile in self._plan.main_sheets:
            ws    = self.wb[profile.name]
            items = self._extract_data_sheet(ws, profile)
            raw_items.extend(items)
            annexure_stats[profile.name] = len(items)

        # Continuation sheets
        for profile in self._plan.continuation_sheets:
            ws    = self.wb[profile.name]
            items = self._extract_data_sheet(ws, profile)
            raw_items.extend(items)
            annexure_stats[profile.name] = len(items)

        # Annexure sheets
        for profile in self._plan.annexure_sheets:
            ws    = self.wb[profile.name]
            items = self._extract_data_sheet(ws, profile)
            raw_items.extend(items)
            annexure_stats[profile.name] = len(items)

        if not raw_items:
            log.warning("Extraction plan ran but zero items extracted")
            return self._empty_result()

        # 4. Duplicate detection
        dup_labels = compute_duplicate_ids(raw_items)
        for item, label in zip(raw_items, dup_labels):
            item['duplicate_id'] = label if label else 0

        # 5. Build output rows
        out_rows = self._build_output_rows(raw_items)

        ctx = self._global_context
        total_tags   = len({item.get('tag_no') for item in raw_items if item.get('tag_no')})
        spare_items  = len({item.get('item_num') for item in raw_items if item.get('item_num')})

        return {
            'format':         'FORMAT_ADAPTIVE',
            'spir_no':        ctx.get('spir_no', ''),
            'equipment':      ctx.get('equipment', ''),
            'manufacturer':   ctx.get('manufacturer', ''),
            'supplier':       ctx.get('supplier', ''),
            'spir_type':      ctx.get('spir_type'),
            'eqpt_qty':       ctx.get('eqpt_qty', total_tags),
            'spare_items':    spare_items,
            'total_tags':     total_tags,
            'annexure_count': len(self._plan.continuation_sheets) + len(self._plan.annexure_sheets),
            'annexure_stats': annexure_stats,
            'rows':           out_rows,
        }

    # ── Global context extraction ─────────────────────────────────────────────

    def _extract_global_context(self) -> None:
        """
        Extract workbook-level metadata (SPIR number, manufacturer, etc.)
        from the first main sheet.  Scans rows 1..header_row-1 for known patterns.
        """
        if not self._plan.main_sheets:
            return

        profile = self._plan.main_sheets[0]
        ws      = self.wb[profile.name]
        scan_to = (profile.header_row or 8)

        ctx: dict[str, Any] = {
            'spir_no':      '',
            'equipment':    '',
            'manufacturer': '',
            'supplier':     '',
            'spir_type':    None,
            'eqpt_qty':     None,
        }

        spir_pattern   = re.compile(r'VEN-\d{4}-\S+', re.IGNORECASE)
        vendor_pattern = re.compile(r'(?:supplier|vendor|manufacturer)\s*:?\s*(.+)', re.IGNORECASE)

        for ri in range(1, scan_to + 1):
            for ci in range(1, ws.max_column + 1):
                raw = ws.cell(ri, ci).value
                if raw is None:
                    continue
                s = str(raw).strip()
                if not s:
                    continue

                # SPIR number
                if not ctx['spir_no']:
                    m = spir_pattern.search(s)
                    if m:
                        ctx['spir_no'] = m.group(0).split('\n')[0].strip()

                # Manufacturer / supplier
                vm = vendor_pattern.match(s)
                if vm:
                    val = vm.group(1).strip()
                    if _real(val):
                        sl = s.lower()
                        if 'manuf' in sl and not ctx['manufacturer']:
                            ctx['manufacturer'] = val
                        elif 'supplier' in sl and not ctx['supplier']:
                            ctx['supplier'] = val

                # SPIR type from checkbox-like cell
                if any(kw in s.lower() for kw in ('commissioning', 'initial spare',
                                                    'normal operat', 'life cycle')):
                    # Look for a truthy value in the next cell
                    neighbor = ws.cell(ri, ci + 1).value
                    if neighbor in (True, 1, 'x', 'X', 'yes', 'YES', '1'):
                        sl = s.lower()
                        if 'commission' in sl:
                            ctx['spir_type'] = 'Commissioning Spares'
                        elif 'initial' in sl:
                            ctx['spir_type'] = 'Initial Spares'
                        elif 'normal' in sl:
                            ctx['spir_type'] = 'Normal Operating Spares'
                        elif 'life' in sl:
                            ctx['spir_type'] = 'Life Cycle Spares'

                # Equipment description (long text in early rows)
                if not ctx['equipment'] and len(s) > 30 and ri <= 3:
                    ctx['equipment'] = s[:200]

        self._global_context = ctx
        log.debug("Global context: %s", {k: v for k, v in ctx.items() if v})

    # ── Per-sheet extraction ──────────────────────────────────────────────────

    def _extract_data_sheet(self, ws, profile: SheetProfile) -> list[dict]:
        """
        Extract all data rows from a single sheet using dynamic column mapping.

        Returns a list of dicts, one per item row.  Each dict uses the
        internal field names from OUTPUT_SCHEMA.
        """
        if profile.header_row is None:
            log.warning("Sheet '%s': no header row — skipping", profile.name)
            return []

        hdr = profile.header_row
        # Scan hdr ± 1 for resilience against merged cells
        scan_rows = [r for r in [hdr - 1, hdr, hdr + 1] if 1 <= r <= ws.max_row]
        mapper    = map_sheet(ws, header_rows=scan_rows)

        # Build col_idx lookup: internal_field → column index
        col_map: dict[str, int | None] = {}
        for field_name, mapper_key in _FIELD_TO_MAPPER.items():
            col_map[field_name] = mapper.get(mapper_key)

        items:     list[dict] = []
        sheet_lbl = ws.title.upper().strip()

        for ri in range(hdr + 1, ws.max_row + 1):
            # Skip if item number and description are both absent
            item_num_val = None
            if col_map.get('item_num'):
                item_num_val = _cn(ws, ri, col_map['item_num'])

            desc = ''
            if col_map.get('desc'):
                desc = _cv(ws, ri, col_map['desc'])

            if item_num_val is None and not desc:
                continue                  # empty row
            if _is_footer_row(desc):
                break                     # hit footer — stop scanning

            # Read all mapped fields
            item: dict[str, Any] = {
                'sheet':    sheet_lbl,
                'spir_no':  self._global_context.get('spir_no', ''),
                'spir_type': self._global_context.get('spir_type'),
            }

            for field_name, ci in col_map.items():
                if ci is None:
                    item[field_name] = None
                    continue
                # Use numeric reading for numeric-expected fields
                if field_name in ('item_num', 'qty_identical', 'eqpt_qty',
                                   'unit_price', 'delivery'):
                    item[field_name] = _cn(ws, ri, ci)
                else:
                    raw = _cv(ws, ri, ci)
                    item[field_name] = raw if raw else None

            # item_num may not have been read yet if col_map had it
            if 'item_num' not in item or item['item_num'] is None:
                item['item_num'] = item_num_val
            if 'desc' not in item or item['desc'] is None:
                item['desc'] = desc or None

            # Build NEW DESCRIPTION
            item['new_desc'] = make_new_desc(
                item.get('desc') or '',
                item.get('mfr_part_no'),
                item.get('supplier_name'),
            )

            # Fill tag_no from global context if not on this sheet
            if not item.get('tag_no'):
                item['tag_no'] = self._global_context.get('tag_no')

            # Skip rows with no meaningful content at all
            meaningful = any(
                _real(item.get(f))
                for f in ('item_num', 'desc', 'mfr_part_no', 'sap_no')
            )
            if not meaningful:
                continue

            items.append(item)

        log.debug("Sheet '%s': extracted %d items", ws.title, len(items))
        return items

    # ── Output row builder ────────────────────────────────────────────────────

    def _build_output_rows(self, raw_items: list[dict]) -> list[list[Any]]:
        """
        Convert internal item dicts to OUTPUT_COLS-ordered rows.
        This is the ONLY place that references OUTPUT_SCHEMA, so adding
        a new output column only requires updating OUTPUT_SCHEMA above.
        """
        out_rows = []
        for item in raw_items:
            row = [None] * len(OUTPUT_COLS)
            for schema_entry in OUTPUT_SCHEMA:
                col_name   = schema_entry["col"]
                field_name = schema_entry["field"]
                default    = schema_entry["default"]
                col_idx    = CI[col_name]
                row[col_idx] = item.get(field_name, default)
            out_rows.append(row)
        return out_rows

    def _empty_result(self) -> dict:
        return {
            'format':         'FORMAT_ADAPTIVE',
            'spir_no':        '',
            'equipment':      '',
            'manufacturer':   '',
            'supplier':       '',
            'spir_type':      None,
            'eqpt_qty':       0,
            'spare_items':    0,
            'total_tags':     0,
            'annexure_count': 0,
            'annexure_stats': {},
            'rows':           [],
        }


# ── Dispatcher — single entry point for the whole system ─────────────────────

def spir_dispatcher(wb) -> dict:
    """
    Master dispatcher.  This replaces extract_spir() as the single entry point.

    Strategy:
    1. Try the battle-tested known-format engine (FORMAT1-FORMAT5)
    2. If that succeeds with data → return its result (no change to existing behaviour)
    3. If that returns zero rows → try the adaptive extractor
    4. If the format is not recognised → use the adaptive extractor directly

    This guarantees:
    • All 5 existing formats continue to work exactly as before
    • New formats are handled automatically without new parser code
    • The caller receives the same dict structure regardless of path taken
    """
    try:
        fmt = _engine_detect_format(wb)
        result = _engine_extract(wb)

        if result['rows']:
            log.info("Known engine: format=%s  rows=%d", fmt, len(result['rows']))
            return result

        # Engine ran but returned nothing — unusual, fall through
        log.warning(
            "Known engine returned 0 rows for format=%s — "
            "falling back to adaptive extractor", fmt
        )

    except Exception as exc:
        log.warning("Known engine raised %s — falling back to adaptive extractor", exc)

    # Adaptive path
    log.info("Running adaptive extractor on '%s'", wb.sheetnames)
    result = AdaptiveExtractor(wb).extract()
    log.info(
        "Adaptive extractor: format=%s  rows=%d  tags=%d",
        result['format'], len(result['rows']), result['total_tags'],
    )
    return result
