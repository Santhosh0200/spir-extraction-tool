"""
extraction/sheet_classifier.py
────────────────────────────────
Sheet Type Classifier
──────────────────────
Determines the role of every sheet in a workbook:

  MAIN         — primary equipment + spare parts data
  CONTINUATION — overflow rows referencing main sheet items
  ANNEXURE     — per-equipment sheets in FORMAT1 style
  VALIDATION   — lookup / dropdown reference lists (not data)
  UNKNOWN      — cannot be determined

Two strategies are used in combination:

  1. NAME-BASED  — fast, checks sheet name for keywords
  2. CONTENT-BASED — slower, checks cell content when name is ambiguous

The result is a SheetProfile per sheet containing the role, confidence,
detected header row, and a column-mapping coverage score.  This drives
the adaptive extractor to know what to do with each sheet.

Public API
──────────
    classify_workbook(wb)                      → dict[name, SheetProfile]
    classify_sheet(ws, name)                   → SheetProfile
    get_extraction_plan(profiles)              → ExtractionPlan
"""
from __future__ import annotations
import re
import logging
from dataclasses import dataclass, field
from enum import Enum

from extraction.header_detector import detect_header_row, detect_header_row_with_scores
from extraction.column_mapper import map_sheet, ColumnMapper

log = logging.getLogger(__name__)


# ── Sheet roles ───────────────────────────────────────────────────────────────

class SheetRole(str, Enum):
    MAIN         = "main"
    CONTINUATION = "continuation"
    ANNEXURE     = "annexure"
    VALIDATION   = "validation"
    UNKNOWN      = "unknown"


# ── Name-based keyword rules ──────────────────────────────────────────────────
# Each entry: (keywords_that_must_all_appear, role, confidence)

_NAME_RULES: list[tuple[list[str], SheetRole, float]] = [
    # High-confidence name matches
    (["main"],                        SheetRole.MAIN,         0.95),
    (["continuation"],                SheetRole.CONTINUATION, 0.95),
    (["annexure"],                    SheetRole.ANNEXURE,     0.95),
    (["annex"],                       SheetRole.ANNEXURE,     0.90),
    # Validation / lookup sheets — never data
    (["validation"],                  SheetRole.VALIDATION,   0.99),
    (["lookup"],                      SheetRole.VALIDATION,   0.99),
    (["list"],                        SheetRole.VALIDATION,   0.80),
    (["reference"],                   SheetRole.VALIDATION,   0.80),
    (["drop"],                        SheetRole.VALIDATION,   0.85),
    # Weaker main-sheet signals
    (["spare"],                       SheetRole.MAIN,         0.70),
    (["spir"],                        SheetRole.MAIN,         0.75),
    (["sheet1"],                      SheetRole.MAIN,         0.50),  # Excel default
    (["sheet 1"],                     SheetRole.MAIN,         0.50),
]

# Minimum column-mapping coverage to treat a sheet as data (not validation)
_MIN_DATA_COVERAGE = 0.05   # at least 5% of known SPIR fields present


@dataclass
class SheetProfile:
    """Full characterisation of a single worksheet."""
    name:           str
    role:           SheetRole
    confidence:     float           # 0.0-1.0 how sure we are of the role
    header_row:     int | None      # detected header row (1-based), or None
    col_coverage:   float           # fraction of SPIR fields mapped by ColumnMapper
    col_map:        dict            # field_name → {col, letter, text, score}
    row_count:      int             # ws.max_row
    col_count:      int             # ws.max_column
    warnings:       list[str] = field(default_factory=list)

    @property
    def is_data_sheet(self) -> bool:
        return self.role in (SheetRole.MAIN, SheetRole.CONTINUATION, SheetRole.ANNEXURE)

    @property
    def summary(self) -> dict:
        return {
            "role":         self.role.value,
            "confidence":   round(self.confidence, 2),
            "header_row":   self.header_row,
            "col_coverage": round(self.col_coverage, 2),
            "row_count":    self.row_count,
            "col_count":    self.col_count,
            "warnings":     self.warnings,
        }


@dataclass
class ExtractionPlan:
    """
    Ordered plan produced by get_extraction_plan().
    Tells the adaptive extractor exactly what to do with each sheet.
    """
    main_sheets:         list[SheetProfile]   # process first
    continuation_sheets: list[SheetProfile]   # process second
    annexure_sheets:     list[SheetProfile]   # process third
    skipped_sheets:      list[SheetProfile]   # validation / unknown — skip
    total_data_sheets:   int

    @property
    def has_data(self) -> bool:
        return self.total_data_sheets > 0

    def describe(self) -> str:
        lines = ["ExtractionPlan:"]
        for sp in self.main_sheets:
            lines.append(f"  MAIN         '{sp.name}'  hdr={sp.header_row}  cov={sp.col_coverage:.0%}")
        for sp in self.continuation_sheets:
            lines.append(f"  CONTINUATION '{sp.name}'  hdr={sp.header_row}  cov={sp.col_coverage:.0%}")
        for sp in self.annexure_sheets:
            lines.append(f"  ANNEXURE     '{sp.name}'  hdr={sp.header_row}  cov={sp.col_coverage:.0%}")
        for sp in self.skipped_sheets:
            lines.append(f"  SKIP         '{sp.name}'  ({sp.role.value})")
        return "\n".join(lines)


# ── Core classifier ───────────────────────────────────────────────────────────

def classify_sheet(ws, name: str | None = None) -> SheetProfile:
    """
    Classify a single worksheet and return its SheetProfile.

    Strategy:
    1. Try name-based rules first (fast, high confidence)
    2. If name is ambiguous, use content analysis:
       a. Detect header row
       b. Run ColumnMapper for coverage score
       c. Use coverage to decide between MAIN / VALIDATION / UNKNOWN
    """
    sheet_name = name or ws.title
    sl         = sheet_name.lower().strip()
    warnings   = []

    # ── 1. Name-based classification ─────────────────────────────────────────
    name_role:   SheetRole | None = None
    name_conf:   float            = 0.0

    for keywords, role, conf in _NAME_RULES:
        if all(kw in sl for kw in keywords):
            if conf > name_conf:
                name_role = role
                name_conf = conf

    # If name says VALIDATION with high confidence, skip content analysis
    if name_role == SheetRole.VALIDATION and name_conf >= 0.95:
        return SheetProfile(
            name         = sheet_name,
            role         = SheetRole.VALIDATION,
            confidence   = name_conf,
            header_row   = None,
            col_coverage = 0.0,
            col_map      = {},
            row_count    = ws.max_row or 0,
            col_count    = ws.max_column or 0,
        )

    # ── 2. Content-based analysis ─────────────────────────────────────────────
    header_row = detect_header_row(ws)
    mapper     = None
    coverage   = 0.0
    col_map    = {}

    if header_row is not None:
        # Scan the detected header row ± 1 for resilience
        scan_rows = [r for r in [header_row - 1, header_row, header_row + 1]
                     if 1 <= r <= ws.max_row]
        mapper    = map_sheet(ws, header_rows=scan_rows)
        coverage  = mapper.coverage()
        col_map   = {k: v for k, v in mapper.report().items() if v is not None}
    else:
        warnings.append(f"No header row detected in '{sheet_name}'")

    # ── 3. Resolve role ───────────────────────────────────────────────────────
    if name_role is not None and name_conf >= 0.70:
        # Name gave us a strong enough answer
        final_role = name_role
        final_conf = name_conf
    elif coverage < _MIN_DATA_COVERAGE:
        # Very low column coverage → likely not a data sheet
        if name_role == SheetRole.MAIN:
            # Even if named 'main', low coverage is suspicious
            final_role = SheetRole.MAIN
            final_conf = name_conf * 0.5
            warnings.append(
                f"Sheet '{sheet_name}' named 'main' but has low SPIR column coverage "
                f"({coverage:.0%}). May be a non-data sheet."
            )
        else:
            final_role = SheetRole.VALIDATION if header_row is None else SheetRole.UNKNOWN
            final_conf = 0.4
    else:
        # Content analysis has enough data — determine role from coverage + name hints
        if name_role in (SheetRole.CONTINUATION, SheetRole.ANNEXURE):
            final_role = name_role
            final_conf = max(name_conf, 0.70 + coverage * 0.2)
        else:
            # Default: treat as MAIN if it has good coverage and no other signal
            final_role = SheetRole.MAIN
            final_conf = 0.60 + coverage * 0.3

    profile = SheetProfile(
        name         = sheet_name,
        role         = final_role,
        confidence   = min(1.0, final_conf),
        header_row   = header_row,
        col_coverage = coverage,
        col_map      = col_map,
        row_count    = ws.max_row or 0,
        col_count    = ws.max_column or 0,
        warnings     = warnings,
    )

    log.debug(
        "Classified '%s': role=%s  conf=%.2f  hdr=%s  cov=%.0%%",
        sheet_name, final_role.value, final_conf, header_row, coverage * 100,
    )
    return profile


def classify_workbook(wb) -> dict[str, SheetProfile]:
    """
    Classify every sheet in the workbook.

    Returns:
        dict mapping sheet_name → SheetProfile for all sheets.
    """
    profiles = {}
    for name in wb.sheetnames:
        ws = wb[name]
        profiles[name] = classify_sheet(ws, name)
    return profiles


def get_extraction_plan(profiles: dict[str, SheetProfile]) -> ExtractionPlan:
    """
    Convert a profile dict into an ordered ExtractionPlan.

    Ordering rules:
    • Main sheets first, sorted by confidence desc
    • Continuation sheets second (order preserved from workbook)
    • Annexure sheets third (order preserved)
    • Everything else is skipped
    """
    mains   = sorted(
        [p for p in profiles.values() if p.role == SheetRole.MAIN],
        key=lambda p: -p.confidence,
    )
    conts   = [p for p in profiles.values() if p.role == SheetRole.CONTINUATION]
    annexes = [p for p in profiles.values() if p.role == SheetRole.ANNEXURE]
    skipped = [p for p in profiles.values()
               if p.role in (SheetRole.VALIDATION, SheetRole.UNKNOWN)]

    plan = ExtractionPlan(
        main_sheets         = mains,
        continuation_sheets = conts,
        annexure_sheets     = annexes,
        skipped_sheets      = skipped,
        total_data_sheets   = len(mains) + len(conts) + len(annexes),
    )
    log.info("ExtractionPlan: %s", plan.describe())
    return plan
