"""Generate synthetic RiskMatch API CSV exports.

RiskMatch provides structured CSV exports across 3 reporting periods:
MTD (month-to-date), YTD (year-to-date), TTM (trailing twelve months).
Each period has the same column structure but different date ranges.
"""

import csv
import os
from datetime import datetime

from faker import Faker

from constants import (
    AGENCY_CODES, BUSINESS_NAMES, CARRIER_NAMES, COVERAGE_TYPES,
    US_STATES, seeded_random,
)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output", "riskmatch")
fake = Faker()
Faker.seed(42)
rng = seeded_random()

HEADERS = [
    "report_period",
    "agency_code",
    "carrier_name",
    "insured_name",
    "policy_number",
    "line_of_business",
    "effective_date",
    "expiration_date",
    "state",
    "written_premium",
    "earned_premium",
    "commission_rate",
    "commission_amount",
    "new_vs_renewal",
    "producer_name",
    "report_date",
]


def generate_period_file(period, row_count, date_range):
    """Generate a single RiskMatch CSV for one reporting period."""
    start_date, end_date = date_range
    filename = f"riskmatch_{period.lower()}_{datetime.now().strftime('%Y%m')}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)

        for _ in range(row_count):
            written_premium = round(rng.uniform(1000, 500000), 2)
            commission_rate = round(rng.uniform(0.08, 0.20), 4)
            earned_ratio = rng.uniform(0.3, 1.0) if period != "MTD" else rng.uniform(0.05, 0.30)

            eff_date = fake.date_between(start_date=start_date, end_date=end_date)
            writer.writerow([
                period,
                rng.choice(AGENCY_CODES),
                rng.choice(CARRIER_NAMES),
                rng.choice(BUSINESS_NAMES),
                f"RM-{rng.randint(100000, 999999)}",
                rng.choice(COVERAGE_TYPES),
                eff_date.isoformat(),
                eff_date.replace(year=eff_date.year + 1).isoformat(),
                rng.choice(US_STATES),
                written_premium,
                round(written_premium * earned_ratio, 2),
                commission_rate,
                round(written_premium * commission_rate, 2),
                rng.choice(["New", "Renewal"]),
                fake.name(),
                datetime.now().strftime("%Y-%m-%d"),
            ])

    print(f"✓ {filename} ({row_count} rows)")


def main():
    """Generate all RiskMatch CSV exports."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("── Generating RiskMatch files ──")

    periods = {
        "MTD": (120, ("-30d", "today")),
        "YTD": (500, ("-365d", "today")),
        "TTM": (800, ("-365d", "today")),
    }

    for period, (count, dates) in periods.items():
        generate_period_file(period, count, dates)

    print(f"✓ RiskMatch fixtures written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
