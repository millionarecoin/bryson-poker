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
from typing import List, Optional

import requests
import pandas as pd
from dateutil import parser as dateparser

# -----------------------------
# CONFIG
# -----------------------------

# Recommended: set env var instead of hardcoding
# PowerShell: setx SPLITWISE_API_KEY "your_key_here"
SPLITWISE_API_KEY = os.getenv("SPLITWISE_API_KEY") or "f8xBSXMD8T3iYa51JzSuGMpMFbATund2I2vweKjc"

GROUP_ID = 70730375
TARGET_YEAR = 2026

# Exclude these from calculations (case-insensitive substring match)
EXCLUDE_DESCRIPTION_KEYWORDS = [
    "settle all balances",
    "poker mat",
    "SNP Chairs",
    "Payment",
    "Table and chairs",
    "Poker table", "Copag cards", "Beer", "Food", "Drinks", "Cake", "Cards", "Pakoda"
]

OUTPUT_DIR = Path("outputs")
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

def parse_expenses(expenses: List[dict], target_year: Optional[int] = TARGET_YEAR) -> pd.DataFrame:
    rows = []

    for exp in expenses:
        if should_exclude_expense(exp):
            continue

        dt = dateparser.parse(exp["date"])
        if target_year is not None and dt.year != target_year:
            continue

        desc = exp.get("description", "")

        # Each expense has users with paid_share & owed_share
        for u in exp.get("users", []):
            user_obj = u.get("user", {}) or {}
            first = (user_obj.get("first_name") or "").strip()
            last = (user_obj.get("last_name") or "").strip()
            full = f"{first} {last}".strip()

            # Fallbacks if last name is missing
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

def write_xlsx(yearly, weekly_winners, weekly, raw, alltime_yearly=None, alltime_weekly_winners=None, alltime_weekly=None, raw_all=None):
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
        # Yearly/all-time leaderboard sheets
        if alltime_yearly is not None:
            alltime_yearly.to_excel(w, sheet_name="Alltime Leaderboard", index=False)

        # All-time weekly winners/totals and optional raw all-time rows
        if alltime_weekly_winners is not None:
            alltime_weekly_winners.to_excel(w, sheet_name="Alltime Weekly Winners", index=False)
        if alltime_weekly is not None:
            alltime_weekly.to_excel(w, sheet_name="Alltime Weekly Totals", index=False)
        if raw_all is not None:
            raw_all.to_excel(w, sheet_name="Alltime Raw Rows", index=False)

    return path


# -----------------------------
# MAIN
# -----------------------------

def main():
    if not SPLITWISE_API_KEY or SPLITWISE_API_KEY == "PASTE_YOUR_KEY_HERE":
        raise RuntimeError("Set SPLITWISE_API_KEY env var or paste the key in the script temporarily.")

    expenses = fetch_group_expenses(GROUP_ID)
    print(f"Fetched {len(expenses)} expenses (before filtering)")

    # Per-year (TARGET_YEAR) and all-time
    df_year = parse_expenses(expenses, TARGET_YEAR)
    df_all = parse_expenses(expenses, None)

    if df_year.empty and df_all.empty:
        print("No qualifying expenses found after filtering.")
        return

    # Excel can't write timezone-aware datetimes
    if not df_year.empty:
        df_year["date"] = pd.to_datetime(df_year["date"], errors="coerce").dt.tz_localize(None)
    if not df_all.empty:
        df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce").dt.tz_localize(None)

    yearly, weekly_winners, weekly = compute_leaderboards(df_year if not df_year.empty else df_all)
    alltime_yearly, alltime_weekly_winners, alltime_weekly = (
        compute_leaderboards(df_all) if not df_all.empty else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    )

    out = write_xlsx(
        yearly,
        weekly_winners,
        weekly,
        df_year if not df_year.empty else None,
        alltime_yearly=alltime_yearly if not alltime_yearly.empty else None,
        alltime_weekly_winners=(alltime_weekly_winners if not alltime_weekly_winners.empty else None),
        alltime_weekly=(alltime_weekly if not alltime_weekly.empty else None),
        raw_all=(df_all if not df_all.empty else None),
    )

    print(f"✅ Done. Created {out}")
    print(f"Kept rows for {TARGET_YEAR}: {len(df_year)}")
    print(f"Kept rows (all-time): {len(df_all)}")

if __name__ == "__main__":
    main()
