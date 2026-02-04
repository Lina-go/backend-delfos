"""
Test SQL agent date handling patterns.

Verifies whether the SQL agent generates relative (GETDATE/DATEADD)
or absolute (hardcoded) dates for temporal queries. This determines
if saved graphs can be refreshed by re-executing the stored query.

Usage:
    python test_sql_dates.py
    python test_sql_dates.py --url http://localhost:8000
"""

import argparse
import csv
import json
import re
from datetime import datetime

import httpx


TEST_CASES = [
    {"id": 1,  "group": "relative",      "question": "Como ha evolucionado la cartera de consumo bajo monto en los ultimos 6 meses?",                                   "risk": "Could hardcode dates instead of DATEADD"},
    {"id": 2,  "group": "relative",      "question": "Cual fue la tasa de captacion promedio del ultimo trimestre?",                                                     "risk": "Could hardcode the quarter boundaries"},
    {"id": 3,  "group": "relative",      "question": "Cuales son los top 5 bancos por cartera en el ultimo ano?",                                                        "risk": "Could use YEAR=2025 instead of DATEADD"},
    {"id": 4,  "group": "getdate_empty", "question": "Cual es la distribucion de cartera del ultimo mes?",                                                               "risk": "DATEADD(MONTH,-1,GETDATE())=Jan2026 -> 0 rows"},
    {"id": 5,  "group": "getdate_empty", "question": "Como han variado las tasas de CDT en los ultimos 30 dias?",                                                        "risk": "DATEADD(DAY,-30,GETDATE())=Jan2026 -> 0 rows"},
    {"id": 6,  "group": "getdate_empty", "question": "Cual es el saldo de cartera vigente de este ano?",                                                                 "risk": "YEAR(GETDATE())=2026 -> 0 rows"},
    {"id": 7,  "group": "ambiguous",     "question": "Como cambio la cartera de tarjetas de credito entre el segundo y tercer trimestre?",                                "risk": "No explicit year"},
    {"id": 8,  "group": "ambiguous",     "question": "Cual es la tendencia reciente de las tasas de captacion de CDT a 90 dias?",                                        "risk": "'Reciente' is subjective"},
    {"id": 9,  "group": "ambiguous",     "question": "Compara la cartera comercial corporativa de julio vs octubre",                                                     "risk": "No year specified"},
    {"id": 10, "group": "absolute",      "question": "Cual fue la cartera de vivienda VIS en agosto de 2025?",                                                           "risk": "Must stay absolute on refresh"},
    {"id": 11, "group": "absolute",      "question": "Cuanto captaron los bancos el 15 de noviembre de 2025?",                                                           "risk": "Exact date, must stay absolute"},
    {"id": 12, "group": "worst_case",    "question": "Cual es la evolucion de la cartera de microcreditos en lo que va del ano?",                                         "risk": "'Lo que va del ano'=2026 -> 0 rows"},
    {"id": 13, "group": "worst_case",    "question": "Como estan las tasas de captacion hoy?",                                                                           "risk": "'Hoy'=Feb2026 -> no data"},
    {"id": 14, "group": "worst_case",    "question": "Cual fue el crecimiento mensual de cartera de libre inversion del mes pasado vs el anterior?",                      "risk": "'Mes pasado'=Jan2026 -> 0 rows cartera"},
]

RELATIVE_PATTERNS = [
    r"GETDATE\s*\(\s*\)",
    r"DATEADD\s*\(",
    r"CURRENT_TIMESTAMP",
    r"SYSDATETIME\s*\(\s*\)",
]
ABSOLUTE_DATE_RE = re.compile(r"'\d{4}-\d{2}-\d{2}'")
MAX_DATE_RE = re.compile(r"MAX\s*\(\s*(?:FECHA_CORTE|FECHACORTE)\s*\)", re.IGNORECASE)


def analyze_sql(sql):
    """Return date_type and details for a SQL string."""
    if not sql:
        return "NO_SQL", "No SQL generated"

    has_relative = any(re.search(p, sql, re.IGNORECASE) for p in RELATIVE_PATTERNS)
    absolute_dates = ABSOLUTE_DATE_RE.findall(sql)
    has_max = bool(MAX_DATE_RE.search(sql))
    has_where = "WHERE" in sql.upper()

    if has_relative:
        date_type = "RELATIVE"
    elif has_max:
        date_type = "MAX_DATE"
    elif absolute_dates:
        date_type = "ABSOLUTE"
    elif not has_where:
        date_type = "NO_FILTER"
    else:
        date_type = "OTHER"

    parts = []
    if has_relative:
        parts.append("GETDATE/DATEADD")
    if absolute_dates:
        parts.append("dates=" + ",".join(absolute_dates))
    if has_max:
        parts.append("MAX(FECHA_CORTE)")
    details = " | ".join(parts) if parts else "no date logic"

    return date_type, details


def refresh_safe(date_type):
    """SAFE means re-execution returns fresh data. FIXED means it returns the same data."""
    if date_type in ("RELATIVE", "MAX_DATE", "NO_FILTER", "OTHER"):
        return "SAFE"
    if date_type == "ABSOLUTE":
        return "FIXED"
    return "ERROR"


def call_stream(base_url, question):
    """POST to /api/chat/stream, return sql, row_count, error."""
    sql, error, row_count = "", None, 0
    try:
        with httpx.Client(timeout=120.0) as client:
            with client.stream("POST", f"{base_url}/api/chat/stream",
                               json={"message": question, "user_id": "test_dates"}) as resp:
                buf = ""
                for chunk in resp.iter_text():
                    buf += chunk
                    while "\n\n" in buf:
                        raw, buf = buf.split("\n\n", 1)
                        for line in raw.strip().split("\n"):
                            if not line.startswith("data: "):
                                continue
                            try:
                                evt = json.loads(line[6:])
                                if evt.get("step") == "sql_generation":
                                    sql = evt.get("result", {}).get("sql", "")
                                    if evt.get("result", {}).get("error"):
                                        error = evt["result"]["error"]
                                if evt.get("step") == "sql_execution":
                                    row_count = evt.get("result", {}).get("total_filas", 0)
                            except json.JSONDecodeError:
                                pass
    except httpx.TimeoutException:
        error = "TIMEOUT"
    except httpx.ConnectError:
        error = "CONNECTION_ERROR"
    except Exception as exc:
        error = str(exc)
    return sql, row_count, error


def main():
    parser = argparse.ArgumentParser(description="Test SQL agent date patterns")
    parser.add_argument("--url", default="http://localhost:8080", help="Backend URL")
    parser.add_argument("--output", default="test_sql_dates_report.csv", help="CSV output")
    args = parser.parse_args()

    total = len(TEST_CASES)
    print(f"\nSQL Date Pattern Test | {datetime.now():%Y-%m-%d %H:%M} | {args.url} | {total} tests\n")

    rows = []
    for i, tc in enumerate(TEST_CASES, 1):
        label = f"[{i:>2}/{total}] {tc['group']:<14}"
        print(f"{label} {tc['question'][:60]}...")

        sql, row_count, error = call_stream(args.url, tc["question"])

        if error:
            print(f"{'':>20} ERROR: {error}\n")
            rows.append({**tc, "sql": "", "date_type": "ERROR", "details": error,
                         "row_count": 0, "refresh": "ERROR"})
            continue

        date_type, details = analyze_sql(sql)
        ref = refresh_safe(date_type)
        preview = sql[:90].replace("\n", " ") if sql else "(empty)"

        print(f"{'':>20} {date_type:<12} rows={row_count:<6} refresh={ref:<8} {details}")
        print(f"{'':>20} {preview}\n")

        rows.append({**tc, "sql": sql, "date_type": date_type, "details": details,
                     "row_count": row_count, "refresh": ref})

    # Summary
    print("=" * 80)
    print(f"{'#':<4} {'group':<16} {'type':<14} {'rows':<8} {'refresh':<10} {'risk'}")
    print("-" * 80)
    for r in rows:
        print(f"{r['id']:<4} {r['group']:<16} {r['date_type']:<14} "
              f"{r['row_count']:<8} {r['refresh']:<10} {r['risk'][:30]}")

    types = [r["date_type"] for r in rows]
    print(f"\nCounts: RELATIVE={types.count('RELATIVE')} MAX_DATE={types.count('MAX_DATE')} "
          f"ABSOLUTE={types.count('ABSOLUTE')} NO_FILTER={types.count('NO_FILTER')} "
          f"ERROR={types.count('ERROR')}")

    zero = sum(1 for r in rows if r["row_count"] == 0 and r["date_type"] != "ERROR")
    if zero:
        print(f"WARNING: {zero} queries returned 0 rows")

    # CSV
    fields = ["id", "group", "question", "date_type", "row_count", "refresh", "risk", "details", "sql"]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"Report: {args.output}")


if __name__ == "__main__":
    main()