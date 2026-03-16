"""
Splitwise Poker Leaderboard for 2025
- Uses Splitwise Bearer token
- Pulls expenses from ONE group
- Computes net winnings per player
- Excludes:
    - "settle all balances"
    - "poker mat"
    - (optional) Splitwise payment/settlement-type rows if present
- Outputs:
    - Yearly leaderboard
    - Weekly winners
    - Weekly totals
- Writes a NEW XLSX per run
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import List
import json

import requests
import pandas as pd
from dateutil import parser as dateparser

# -----------------------------
# CONFIG
# -----------------------------

# Recommended: set env var instead of hardcoding
# PowerShell: setx SPLITWISE_API_KEY "your_key_here"
SPLITWISE_API_KEY = os.getenv("SPLITWISE_API_KEY") or "f8xBSXMD8T3iYa51JzSuGMpMFbATund2I2vweKjc"

GROUP_IDS = [70730375, 83889064]
#GROUP_IDS = [83889064]
TARGET_YEAR = 2026

# Exclude these from calculations (case-insensitive substring match)
EXCLUDE_DESCRIPTION_KEYWORDS = [
    "settle all balances",
    "poker mat",
    "SNP Chairs",
    "Payment",
    "Pizza",
    "Table and chairs",
    "Poker table", "Beer", "Copag Cards", "Cards", "cards", "Cake", "cake", "Pakoda", "Poker mat"
]

# OUTPUT_DIR = Path("outputs")
from pathlib import Path
OUTPUT_DIR = Path("/www/var/poker/outputs")
BASE_URL = "https://secure.splitwise.com/api/v3.0"

HEADERS = {"Authorization": f"Bearer {SPLITWISE_API_KEY}"}


# -----------------------------
# SPLITWISE API
# -----------------------------


def fetch_group_expenses(group_id: int) -> List[dict]:
    expenses = []
    offset = 0
    limit = 100
    while True:
        resp = requests.get(
            f"{BASE_URL}/get_expenses",
            headers=HEADERS,
            params={"group_id": group_id, "limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json().get("expenses", [])
        if not data:
            break
        expenses.extend(data)
        offset += limit
    return expenses

def fetch_all_group_expenses(group_ids: list) -> list:
    all_expenses = []
    for gid in group_ids:
        all_expenses.extend(fetch_group_expenses(gid))
    # Remove duplicates by expense id (if any overlap)
    seen = set()
    unique_expenses = []
    for exp in all_expenses:
        eid = exp.get("id")
        if eid not in seen:
            unique_expenses.append(exp)
            seen.add(eid)
    return unique_expenses


# -----------------------------
# FILTERS
# -----------------------------

def should_exclude_expense(exp: dict) -> bool:
    desc = (exp.get("description") or "").strip().lower()

    # 1) Exclude by description keywords
    for kw in EXCLUDE_DESCRIPTION_KEYWORDS:
        if kw.lower() in desc:
            return True

    # 2) Exclude payments/settlements if Splitwise marks them
    # (Splitwise sometimes returns settle-ups as "payment": true)
    if exp.get("payment") is True:
        return True

    return False


# -----------------------------
# TRANSFORM
# -----------------------------


def parse_expenses(expenses: List[dict]) -> pd.DataFrame:
    """
    Parse expenses for the TARGET_YEAR only (default behavior for yearly/weekly leaderboards).
    """
    rows = []
    for exp in expenses:
        if should_exclude_expense(exp):
            continue
        dt = dateparser.parse(exp["date"])
        if dt.year != TARGET_YEAR:
            continue
        desc = exp.get("description", "")
        for u in exp.get("users", []):
            user_obj = u.get("user", {}) or {}
            first = (user_obj.get("first_name") or "").strip()
            last = (user_obj.get("last_name") or "").strip()
            full = f"{first} {last}".strip()
            name = (
                full
                if full
                else user_obj.get("name")
                     or user_obj.get("email")
                     or "Unknown"
            )
            paid = float(u.get("paid_share", 0) or 0)
            owed = float(u.get("owed_share", 0) or 0)
            net = paid - owed  # positive = won money
            if net != 0:
                rows.append(
                    {
                        "date": pd.Timestamp(dt),   # timezone fixed later
                        "player": str(name).strip(),
                        "winnings": net,
                        "expense": desc,
                    }
                )
    return pd.DataFrame(rows)


# All-time parser: does NOT filter by year
def parse_expenses_alltime(expenses: List[dict]) -> pd.DataFrame:
    """
    Parse expenses for all years (for all-time leaderboard).
    """
    rows = []
    for exp in expenses:
        if should_exclude_expense(exp):
            continue
        dt = dateparser.parse(exp["date"])
        desc = exp.get("description", "")
        for u in exp.get("users", []):
            user_obj = u.get("user", {}) or {}
            first = (user_obj.get("first_name") or "").strip()
            last = (user_obj.get("last_name") or "").strip()
            full = f"{first} {last}".strip()
            name = (
                full
                if full
                else user_obj.get("name")
                     or user_obj.get("email")
                     or "Unknown"
            )
            paid = float(u.get("paid_share", 0) or 0)
            owed = float(u.get("owed_share", 0) or 0)
            net = paid - owed  # positive = won money
            if net != 0:
                rows.append(
                    {
                        "date": pd.Timestamp(dt),   # timezone fixed later
                        "player": str(name).strip(),
                        "winnings": net,
                        "expense": desc,
                    }
                )
    return pd.DataFrame(rows)


# -----------------------------
# AGGREGATION
# -----------------------------

def add_week_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["month"] = df["date"].dt.strftime("%b")   # Jan, Feb, Mar
    df["week_of_month"] = ((df["date"].dt.day - 1) // 7) + 1

    df["week_label"] = df["month"] + " W" + df["week_of_month"].astype(str)

    return df


def compute_leaderboards(df: pd.DataFrame):
    df = add_week_keys(df)

    yearly = (
        df.groupby("player", as_index=False)["winnings"]
        .sum()
        .sort_values("winnings", ascending=False)
        .reset_index(drop=True)
    )
    yearly["rank"] = range(1, len(yearly) + 1)
    yearly = yearly[["rank", "player", "winnings"]]

    weekly = (
        df.groupby(["week_label", "player"], as_index=False)["winnings"]
        .sum()
        .sort_values(["week_label", "winnings"], ascending=[True, False])
        .reset_index(drop=True)
    )

    weekly_winners = (
        weekly.groupby("week_label", as_index=False)
        .first()
        .rename(columns={"player": "winner", "winnings": "top_winnings"})
    )

    weekly["week_rank"] = (
        weekly.groupby("week_label")["winnings"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    weekly = weekly[["week_label", "week_rank", "player", "winnings"]]

    return yearly, weekly_winners, weekly


# -----------------------------
# OUTPUT
# -----------------------------

def write_xlsx(yearly, weekly_winners, weekly, raw):
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"splitwise_leaderboard_{TARGET_YEAR}_{ts}.xlsx"

    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(
            {
                "status": ["ok"],
                "generated_at": [datetime.now().isoformat()],
                "excluded_keywords": [", ".join(EXCLUDE_DESCRIPTION_KEYWORDS)],
            }
        ).to_excel(w, sheet_name="Info", index=False)

        yearly.to_excel(w, sheet_name="Yearly Leaderboard", index=False)
        weekly_winners.to_excel(w, sheet_name="Weekly Winners", index=False)
        weekly.to_excel(w, sheet_name="Weekly Totals", index=False)
        raw.to_excel(w, sheet_name="Raw Rows", index=False)

    return path


def write_json(yearly, weekly_winners, weekly, raw):
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"splitwise_leaderboard_{TARGET_YEAR}_{ts}.json"
    latest_path = OUTPUT_DIR / "splitwise_leaderboard_latest.json"

    # Format winnings columns to 2 decimals in all tables
    def format_money(df, cols):
        for col in cols:
            if col in df:
                df[col] = df[col].map(lambda x: f"{x:.2f}" if pd.notnull(x) else x)
        return df

    yearly_fmt = format_money(yearly.copy(), ["winnings"])
    weekly_winners_fmt = format_money(weekly_winners.copy(), ["top_winnings"])
    weekly_fmt = format_money(weekly.copy(), ["winnings"])
    raw_fmt = format_money(raw.copy(), ["winnings"])


    # Compute all-time leaderboard from 2024 (combined groups)
    all_expenses = fetch_all_group_expenses(GROUP_IDS)
    all_df = parse_expenses_alltime(all_expenses)
    all_df = all_df[all_df['date'].dt.year >= 2024]
    alltime_yearly, _, _ = compute_leaderboards(all_df)
    alltime_yearly_fmt = format_money(alltime_yearly.copy(), ["winnings"])

    payload = {
        "info": {
            "status": "ok",
            "generated_at": datetime.now().isoformat(),
            "excluded_keywords": ", ".join(EXCLUDE_DESCRIPTION_KEYWORDS),
        },
        "yearly": yearly_fmt.to_dict(orient="records") if yearly is not None else [],
        "weekly_winners": weekly_winners_fmt.to_dict(orient="records") if weekly_winners is not None else [],
        "weekly": weekly_fmt.to_dict(orient="records") if weekly is not None else [],
        "raw": raw_fmt.to_dict(orient="records") if raw is not None else [],
        "alltime": alltime_yearly_fmt.to_dict(orient="records") if not alltime_yearly_fmt.empty else [],
    }

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, default=str)
    # Always write/copy latest
    with open(latest_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, default=str)

    return path


# -----------------------------
# MAIN
# -----------------------------


def main():
    if not SPLITWISE_API_KEY or SPLITWISE_API_KEY == "PASTE_YOUR_KEY_HERE":
        raise RuntimeError("Set SPLITWISE_API_KEY env var or paste the key in the script temporarily.")

    expenses = fetch_all_group_expenses(GROUP_IDS)
    print(f"Fetched {len(expenses)} expenses (before filtering)")

    df = parse_expenses(expenses)
    if df.empty:
        print("No qualifying expenses found for target year after filtering.")
        return

    # Excel can't write timezone-aware datetimes
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

    yearly, weekly_winners, weekly = compute_leaderboards(df)
    out = write_xlsx(yearly, weekly_winners, weekly, df)
    json_out = write_json(yearly, weekly_winners, weekly, df)

    print(f"✅ Done. Created {out}")
    print(f"✅ JSON created: {json_out}")
    print(f"Kept rows: {len(df)} (after filtering)")

if __name__ == "__main__":
    main()
