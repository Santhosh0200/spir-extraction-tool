"""
extraction/annexure_parser.py
──────────────────────────────
Dedicated parser for annexure and continuation sheets.

This module is called by the main extraction engine when it encounters
FORMAT1 (annexure) or FORMAT4/FORMAT5 (continuation) files, but is also
usable standalone for sheet-level inspection and debugging.

Key design:
  • Stateless functions — no class state, easy to test
  • Each function takes an openpyxl worksheet and returns structured dicts
  • Column positions are detected dynamically via ColumnMapper
  • All errors are caught and returned as structured warnings, not exceptions
"""
from __future__ import annotations
import logging
import re
from typing import Any

from extraction.column_mapper import map_sheet, ColumnMapper

log = logging.getLogger(__name__)


# ── Sheet classifier ──────────────────────────────────────────────────────────

def classify_sheet(sheet_name: str) -> str:
    """
    Return one of: 'main' | 'annexure' | 'continuation' | 'validation' | 'unknown'
    based on the sheet name alone.
    """
    sl = sheet_name.lower()
    if any(kw in sl for kw in ("main", "main sheet")):
        return "main"
    if "annexure" in sl:
        return "annexure"
    if "continuation" in sl:
        return "continuation"
    if any(kw in sl for kw in ("validation", "list", "lookup", "ref")):
        return "validation"
    return "unknown"


def get_sheet_roles(wb) -> dict[str, str]:
    """
    Return a mapping of sheet_name → role for every sheet in the workbook.

    Example:
        {
            'MAIN SHEET': 'main',
            'Annexure 1': 'annexure',
            'Continuation Sheet(1)': 'continuation',
            'Validation Lists': 'validation',
        }
    """
    return {name: classify_sheet(name) for name in wb.sheetnames}


# ── Annexure sheet parser (FORMAT 1) ─────────────────────────────────────────

def parse_annexure_sheet(ws) -> dict:
    """
    Parse a FORMAT1-style annexure sheet.

    Layout assumptions:
      Row 1: tag number in the expected tag column
      Row 4: MFR model
      Row 6: MFR serial
      Row 7: no. of units
      Row 8+: spare parts data rows

    Returns:
        {
            'sheet_name': str,
            'tag_no':     str,
            'model':      str,
            'serial':     str,
            'qty':        int,
            'items':      list[dict],
            'warnings':   list[str],
        }
    """
    warnings: list[str] = []
    sheet_name = ws.title

    # Dynamic column mapping
    mapper = map_sheet(ws, header_rows=[5, 6, 7])

    # Detect tag column — column C (3) is standard for annexure sheets
    tag_col   = 3
    item_col  = mapper.get("ITEM_NUMBER")    or 7
    desc_col  = mapper.get("DESCRIPTION")    or 9
    dwg_col   = mapper.get("DWG_NO")         or 10
    part_col  = mapper.get("MFR_PART_NO")    or 11
    sup_col   = mapper.get("SUPPLIER_PART_NO") or 12
    supp_col  = mapper.get("SUPPLIER_NAME")  or 15
    curr_col  = mapper.get("CURRENCY")       or 21
    price_col = mapper.get("UNIT_PRICE")     or 22
    del_col   = mapper.get("DELIVERY")       or 23
    mm_col    = mapper.get("MIN_MAX")        or 24
    uom_col   = mapper.get("UOM")            or 25
    sap_col   = mapper.get("SAP_NUMBER")     or 26
    cls_col   = mapper.get("CLASSIFICATION") or 27

    def cv(r, c):
        v = ws.cell(r, c).value
        return str(v).strip() if v is not None else ""

    def cn(r, c):
        v = ws.cell(r, c).value
        if v is None:
            return None
        try:
            f = float(v)
            return int(f) if f == int(f) else f
        except (ValueError, TypeError):
            return None

    # Equipment metadata (rows 1-7)
    tag_no = cv(1, tag_col)
    model  = cv(4, tag_col) or None
    serial = cv(6, tag_col) or None
    qty_r  = cn(7, tag_col)
    qty    = int(qty_r) if qty_r and qty_r > 0 else 1

    if not tag_no:
        warnings.append(f"Sheet '{sheet_name}': no tag number found in C1")

    # Data rows (row 8 onward)
    items: list[dict] = []
    for ri in range(8, ws.max_row + 1):
        item_num_val = cn(ri, item_col)
        desc         = cv(ri, desc_col)

        # Stop at footer rows
        if item_num_val is None and not desc:
            continue
        if desc.lower().startswith(("project", "company", "engineering",
                                    "reminder", "technical data")):
            break
        if item_num_val is None:
            continue

        mfr_part  = cv(ri, part_col) or None
        sup_part  = cv(ri, sup_col)  or None
        supp_name = cv(ri, supp_col) or None

        items.append({
            "item_num":       int(item_num_val),
            "qty_identical":  cn(ri, item_col - 1) if item_col > 1 else None,
            "desc":           desc,
            "dwg_no":         cv(ri, dwg_col)  or None,
            "mfr_part_no":    mfr_part or sup_part,
            "supplier_name":  supp_name,
            "currency":       cv(ri, curr_col)  or None,
            "unit_price":     cn(ri, price_col),
            "delivery":       cn(ri, del_col),
            "min_max":        cv(ri, mm_col)   or None,
            "uom":            cv(ri, uom_col)  or None,
            "sap_no":         cv(ri, sap_col)  or None,
            "classification": cv(ri, cls_col)  or None,
        })

    log.debug("Parsed annexure '%s': tag=%s  items=%d", sheet_name, tag_no, len(items))
    return {
        "sheet_name": sheet_name,
        "tag_no":     tag_no,
        "model":      model,
        "serial":     serial,
        "qty":        qty,
        "items":      items,
        "warnings":   warnings,
    }


# ── Continuation sheet parser (FORMAT 4 / FORMAT 5) ──────────────────────────

def parse_continuation_sheet(ws, item_map: dict[int, dict]) -> list[dict]:
    """
    Parse a FORMAT4/FORMAT5-style continuation sheet that links back to the
    main sheet via item numbers.

    Args:
        ws:        openpyxl worksheet (continuation sheet)
        item_map:  dict[item_number → item_detail] from the main sheet parse

    Returns:
        List of tag dicts, each containing:
        {
            'tag_no':   str,
            'model':    str | None,
            'serial':   str | None,
            'qty':      int,
            'items':    list[int],          # item numbers from main sheet
            'qty_map':  dict[int, int],     # item_num → qty for this tag
            'warnings': list[str],
        }
    """
    warnings: list[str] = []
    sheet_name = ws.title

    # Detect tag columns — they start at C4 in most continuation layouts
    tag_cols: list[tuple[int, str]] = []    # (col_idx, tag_no)

    def cv(r, c):
        v = ws.cell(r, c).value
        return str(v).strip() if v is not None else ""

    def cn_raw(v):
        if v is None:
            return None
        try:
            f = float(v)
            return int(f) if f == int(f) else f
        except (ValueError, TypeError):
            return None

    # Row 1 tag header detection
    row1_label = cv(1, 2).lower()
    tag_start_col = 4 if ("equip" in row1_label or "tag" in row1_label) else 3

    # Scan row 1 for tag values starting at tag_start_col
    for ci in range(tag_start_col, ws.max_column + 1):
        raw = ws.cell(1, ci).value
        if raw is None:
            continue
        s = str(raw).strip()
        if not s:
            continue
        # Stop if we hit a non-tag label column (e.g. REMARKS)
        sl = s.lower()
        if any(kw in sl for kw in ("remark", "note", "ref", "description",
                                    "total", "approved", "checked")):
            break
        tag_cols.append((ci, s))

    if not tag_cols:
        warnings.append(f"Sheet '{sheet_name}': no tag columns found")
        return []

    # Detect remarks/item-ref column (last meaningful column)
    remarks_col = None
    for ci in range(ws.max_column, max(c for c, _ in tag_cols), -1):
        label = cv(1, ci).lower()
        if any(kw in label for kw in ("remark", "item ref", "ref")):
            remarks_col = ci
            break
    if remarks_col is None:
        # Fallback: first col after the last tag col
        remarks_col = max(c for c, _ in tag_cols) + 1

    # Tag metadata (rows 4, 6, 7)
    tag_meta: dict[int, dict] = {}
    for ci, tag_no in tag_cols:
        model_raw  = ws.cell(4, ci).value
        serial_raw = ws.cell(6, ci).value
        qty_raw    = ws.cell(7, ci).value
        qty_val    = cn_raw(qty_raw)
        tag_meta[ci] = {
            "tag_no": tag_no,
            "model":  str(model_raw).strip() if model_raw else None,
            "serial": str(serial_raw).strip() if serial_raw else None,
            "qty":    int(qty_val) if qty_val and qty_val > 0 else 1,
        }

    # Per-column item + qty tracking
    per_col_items: dict[int, list[int]]      = {ci: [] for ci, _ in tag_cols}
    per_col_qty:   dict[tuple[int, int], int] = {}

    # Data rows (row 8+)
    for ri in range(8, ws.max_row + 1):
        # Item reference links back to main sheet
        ref_raw = ws.cell(ri, remarks_col).value
        if ref_raw is None:
            continue
        try:
            item_ref = int(float(ref_raw))
        except (ValueError, TypeError):
            continue

        if item_ref not in item_map:
            warnings.append(
                f"Sheet '{sheet_name}' row {ri}: item_ref={item_ref} not in main sheet"
            )
            continue

        for ci, _ in tag_cols:
            v = ws.cell(ri, ci).value
            try:
                fv = float(v) if v is not None else 0.0
                if fv > 0:
                    qty_clean = int(fv) if fv == int(fv) else fv
                    per_col_items[ci].append(item_ref)
                    per_col_qty[(ci, item_ref)] = qty_clean
            except (ValueError, TypeError):
                pass

    # Build output list — one dict per tag column
    results: list[dict] = []
    for ci, tag_no in tag_cols:
        meta = tag_meta[ci]
        item_nums = per_col_items[ci]
        col_qty = {item_num: per_col_qty.get((ci, item_num), 1)
                   for item_num in item_nums}

        results.append({
            "tag_no":   meta["tag_no"],
            "model":    meta["model"],
            "serial":   meta["serial"],
            "qty":      meta["qty"],
            "items":    item_nums,
            "qty_map":  col_qty,
            "warnings": warnings,
        })

    log.debug(
        "Parsed continuation '%s': %d tags  items_per_tag=%s",
        sheet_name, len(results),
        {r["tag_no"]: len(r["items"]) for r in results},
    )
    return results


# ── Sheet summary (diagnostic) ────────────────────────────────────────────────

def summarise_workbook(wb) -> dict:
    """
    Return a high-level summary of all sheets — useful for debugging
    and for the /inspect API endpoint.
    """
    summary = {}
    for name in wb.sheetnames:
        ws   = wb[name]
        role = classify_sheet(name)
        mapper = map_sheet(ws)
        summary[name] = {
            "role":       role,
            "rows":       ws.max_row,
            "cols":       ws.max_column,
            "coverage":   round(mapper.coverage(), 2),
            "col_map":    {k: v for k, v in mapper.report().items() if v is not None},
        }
    return summary
