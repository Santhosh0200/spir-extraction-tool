"""
extraction/header_detector.py
───────────────────────────────
Automatic Header Row Detector
───────────────────────────────
Scans the first N rows of a worksheet and scores each row based on how
many SPIR-relevant header keywords it contains.  The highest-scoring row
is returned as the header row.

This solves the problem of SPIR files where:
  • Headers appear at row 3, 6, 8, or even row 12
  • There are title / logo rows before the actual column headers
  • Merged cells push the real header down by a few rows
  • Files have blank rows at the top

Algorithm
─────────
1.  Read every cell in rows 1..SCAN_LIMIT
2.  For each row, compute a keyword-match score
3.  The row with the highest score (minimum MIN_SCORE) is the header row
4.  Tie-break: prefer the earliest such row (headers usually appear first)

Public API
──────────
    detect_header_row(ws, scan_limit=25)  → int or None
    score_row(ws, row_idx)                → float
"""
from __future__ import annotations
import re
import logging
from functools import lru_cache

log = logging.getLogger(__name__)

# ── Tuning constants ──────────────────────────────────────────────────────────

SCAN_LIMIT  = 25      # scan up to this many rows
MIN_SCORE   = 2.0     # minimum total score to be considered a header row
MIN_CELLS   = 3       # header row must have at least this many non-empty cells

# ── Keyword catalogue with weights ───────────────────────────────────────────
# Each entry:  (keyword, weight, exact_only)
# exact_only=True  → keyword must match the entire normalised cell value
# exact_only=False → keyword just needs to appear anywhere in the cell text
#
# Higher weight = stronger signal that this is a header row.

_KEYWORDS: list[tuple[str, float, bool]] = [
    # Very strong signals — these rarely appear outside a header row
    ("description",        2.0,  False),
    ("description of",     3.0,  False),
    ("part number",        3.0,  False),
    ("part no",            2.5,  False),
    ("sap number",         3.5,  False),
    ("sap no",             3.0,  False),
    ("item number",        3.0,  False),
    ("item no",            2.5,  False),
    ("tag number",         3.0,  False),
    ("tag no",             2.5,  False),
    ("equipment tag",      3.0,  False),
    ("manufacturer",       2.0,  False),
    ("mfr",                1.5,  False),
    ("unit of measure",    3.0,  False),
    ("unit price",         3.0,  False),
    ("currency",           2.0,  False),
    ("delivery",           1.5,  False),
    ("classification",     2.0,  False),
    ("drawing",            1.5,  False),
    ("dwg",                1.5,  False),
    ("supplier",           1.5,  False),
    ("quantity",           2.0,  False),
    ("qty",                2.0,  False),
    ("identical",          2.0,  False),
    ("serial",             1.5,  False),
    ("model",              1.0,  False),
    ("remarks",            1.5,  False),
    # Weak signals — common English words
    ("no.",                0.5,  False),
    ("spec",               0.5,  False),
    ("material",           0.8,  False),
    ("min",                0.5,  False),
    ("max",                0.5,  False),
    ("uom",                2.0,  True),     # exact: 'uom' alone is strong
    ("p/n",                2.0,  True),
]

# Compile into a lookup for speed
_COMPILED: list[tuple[re.Pattern, float, bool]] = [
    (re.compile(re.escape(kw), re.IGNORECASE), w, exact)
    for kw, w, exact in _KEYWORDS
]


def _normalise(v) -> str:
    """Return a normalised string from a cell value."""
    if v is None:
        return ""
    s = re.sub(r"[\r\n]+", " ", str(v))
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def score_row(ws, row_idx: int) -> float:
    """
    Compute a keyword-match score for a single row.

    Rules:
    • Each cell's text is checked against every keyword.
    • A keyword can only score once per row (not once per cell).
    • Total score = sum of weights of all matched keywords.

    Args:
        ws:       openpyxl worksheet
        row_idx:  1-based row number

    Returns:
        float — total keyword score for the row
    """
    if row_idx < 1 or row_idx > ws.max_row:
        return 0.0

    # Collect all non-empty cell texts in this row
    texts: list[str] = []
    non_empty = 0
    for ci in range(1, ws.max_column + 1):
        raw = _normalise(ws.cell(row_idx, ci).value)
        if raw:
            texts.append(raw.lower())
            non_empty += 1

    if non_empty < MIN_CELLS:
        return 0.0

    # Score: each keyword can fire at most once across all cells
    total_score = 0.0
    matched_keywords: set[str] = set()

    for ci_text in texts:
        for pattern, weight, exact in _COMPILED:
            kw = pattern.pattern  # original keyword string for dedup
            if kw in matched_keywords:
                continue
            if exact:
                if ci_text == pattern.pattern.lower():
                    matched_keywords.add(kw)
                    total_score += weight
            else:
                if pattern.search(ci_text):
                    matched_keywords.add(kw)
                    total_score += weight

    return total_score


def detect_header_row(
    ws,
    scan_limit: int = SCAN_LIMIT,
    min_score:  float = MIN_SCORE,
) -> int | None:
    """
    Scan the first `scan_limit` rows and return the best header row index.

    Returns:
        1-based row index of the detected header row, or None if no row
        scores above `min_score`.

    Example:
        hdr = detect_header_row(ws)   # → 6
        if hdr:
            for ri in range(hdr + 1, ws.max_row + 1):
                ...  # data rows
    """
    best_row:  int | None = None
    best_score: float     = min_score - 0.001   # must beat this

    limit = min(scan_limit, ws.max_row)
    row_scores: list[tuple[int, float]] = []

    for ri in range(1, limit + 1):
        s = score_row(ws, ri)
        if s > 0:
            row_scores.append((ri, s))
        if s > best_score:
            best_score = s
            best_row   = ri

    if best_row is not None:
        log.debug(
            "Header detected: sheet='%s'  row=%d  score=%.1f",
            ws.title, best_row, best_score,
        )
    else:
        log.warning(
            "No header row found in sheet='%s'  (best score=%.1f < min=%.1f). "
            "Top rows: %s",
            ws.title, best_score + 0.001, min_score,
            sorted(row_scores, key=lambda x: -x[1])[:5],
        )

    return best_row


def detect_header_row_with_scores(
    ws,
    scan_limit: int = SCAN_LIMIT,
) -> list[tuple[int, float]]:
    """
    Return all rows with their scores, sorted by score descending.
    Useful for diagnostics and the /inspect API endpoint.

    Returns:
        [(row_idx, score), ...] sorted by score descending
    """
    limit = min(scan_limit, ws.max_row)
    scores = [(ri, score_row(ws, ri)) for ri in range(1, limit + 1)]
    return sorted(((ri, s) for ri, s in scores if s > 0), key=lambda x: -x[1])
