"""
services/duplicate_checker.py
Post-extraction duplicate analysis service.

Reads the rows produced by the engine and returns counts / summaries
without needing to re-parse the workbook.
"""
from __future__ import annotations
from extraction.spir_engine import CI, OUTPUT_COLS


def analyse_duplicates(rows: list[list]) -> dict:
    """
    Scan extracted rows and return duplicate / SAP-mismatch summary.

    Returns:
        {
            'dup1_count':  int,   # rows flagged as 'Duplicate N'
            'sap_count':   int,   # rows flagged as 'SAP NUMBER MISMATCH'
            'dup_items':   list[dict],  # detail per flagged item
        }
    """
    dup_col = CI['DUPLICATE ID']
    dup1_count = 0
    sap_count  = 0
    dup_items  = []

    for row in rows:
        label = row[dup_col]
        if not label or label == 0:
            continue

        label_str = str(label)

        if label_str.startswith('Duplicate'):
            dup1_count += 1
            dup_items.append({
                'type':        'DUPLICATE',
                'label':       label_str,
                'tag':         row[CI['TAG NO']],
                'item':        row[CI['ITEM NUMBER']],
                'description': row[CI['DESCRIPTION OF PARTS']],
                'part_no':     row[CI['MANUFACTURER PART NUMBER']],
                'sap':         row[CI['SAP NUMBER']],
            })

        elif label_str == 'SAP NUMBER MISMATCH':
            sap_count += 1
            dup_items.append({
                'type':        'SAP_MISMATCH',
                'label':       label_str,
                'tag':         row[CI['TAG NO']],
                'item':        row[CI['ITEM NUMBER']],
                'description': row[CI['DESCRIPTION OF PARTS']],
                'part_no':     row[CI['MANUFACTURER PART NUMBER']],
                'sap':         row[CI['SAP NUMBER']],
            })

    return {
        'dup1_count': dup1_count,
        'sap_count':  sap_count,
        'dup_items':  dup_items,
    }
