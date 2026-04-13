"""Generate synthetic carrier data files for core_carriers domain.

Each carrier reports in its own proprietary format. This generator
produces files for 5 starter carriers to demonstrate the pattern:
  - Acme Insurance Co       → CSV (policy-level)
  - Liberty National Group  → CSV (commission statement)
  - Summit Underwriters     → JSON (claims feed)
  - Beacon Mutual           → Excel (premium bordereau)
  - Patriot Indemnity       → CSV (endorsement log)

Every file has a completely different schema — the Bronze layer
absorbs all differences into raw_row_variant.
"""

import csv
import json
import os
from datetime import datetime, timedelta

from openpyxl import Workbook
from faker import Faker

from constants import (
    AGENCY_CODES, BUSINESS_NAMES, CARRIER_NAMES, COVERAGE_TYPES,
    US_STATES, seeded_random,
)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output", "carriers")
fake = Faker()
Faker.seed(42)
rng = seeded_random()


def generate_acme_csv():
    """Acme Insurance Co — policy-level CSV extract."""
    carrier = "Acme Insurance Co"
    filename = f"acme_policies_{datetime.now().strftime('%Y%m')}.csv"
    filepath = os.path.join(OUTPUT_DIR, "acme", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    headers = [
        "CarrierPolicyID", "AgencyCode", "InsuredName", "InsuredAddress",
        "InsuredCity", "InsuredState", "InsuredZip", "PolicyType",
        "EffDate", "ExpDate", "AnnualPremium", "TransactionType",
        "TransactionDate", "BillingMethod", "ProducerCode",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _ in range(rng.randint(200, 350)):
            eff = fake.date_between(start_date="-1y", end_date="+3m")
            writer.writerow([
                f"ACM-{rng.randint(1000000, 9999999)}",
                rng.choice(AGENCY_CODES),
                rng.choice(BUSINESS_NAMES),
                fake.street_address(),
                fake.city(),
                rng.choice(US_STATES),
                fake.zipcode(),
                rng.choice(COVERAGE_TYPES),
                eff.isoformat(),
                (eff + timedelta(days=365)).isoformat(),
                round(rng.uniform(1500, 250000), 2),
                rng.choice(["New Business", "Renewal", "Endorsement", "Cancellation"]),
                fake.date_between(start_date="-30d", end_date="today").isoformat(),
                rng.choice(["Agency Bill", "Direct Bill"]),
                f"P{rng.randint(100, 999)}",
            ])

    print(f"✓ acme/{filename} ({_count_csv_rows(filepath)} rows)")


def generate_liberty_csv():
    """Liberty National Group — commission statement CSV."""
    carrier = "Liberty National Group"
    filename = f"liberty_commissions_{datetime.now().strftime('%Y%m')}.csv"
    filepath = os.path.join(OUTPUT_DIR, "liberty", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    headers = [
        "Statement_Date", "Agency_ID", "Policy_Num", "Insured",
        "Trans_Type", "Line", "Premium", "Comm_Rate", "Comm_Amt",
        "Carrier_Code", "Pay_Period",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _ in range(rng.randint(300, 500)):
            premium = round(rng.uniform(500, 100000), 2)
            rate = round(rng.uniform(0.08, 0.18), 4)
            writer.writerow([
                fake.date_between(start_date="-60d", end_date="today").isoformat(),
                rng.choice(AGENCY_CODES),
                f"LNG-{rng.randint(100000, 999999)}",
                rng.choice(BUSINESS_NAMES),
                rng.choice(["NB", "RN", "EP", "AP", "RP"]),
                rng.choice(COVERAGE_TYPES[:5]),
                premium,
                rate,
                round(premium * rate, 2),
                "LIBERTY",
                rng.choice(["Monthly", "Quarterly"]),
            ])

    print(f"✓ liberty/{filename} ({_count_csv_rows(filepath)} rows)")


def generate_summit_json():
    """Summit Underwriters — claims feed as JSON lines."""
    carrier = "Summit Underwriters"
    filename = f"summit_claims_{datetime.now().strftime('%Y%m')}.json"
    filepath = os.path.join(OUTPUT_DIR, "summit", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    records = []
    for _ in range(rng.randint(100, 200)):
        paid = round(rng.uniform(0, 200000), 2)
        reserved = round(rng.uniform(0, 150000), 2)
        records.append({
            "claim_id": f"SU-CLM-{rng.randint(100000, 999999)}",
            "policy_id": f"SU-POL-{rng.randint(10000, 99999)}",
            "agency": rng.choice(AGENCY_CODES),
            "insured": rng.choice(BUSINESS_NAMES),
            "loss_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "report_date": fake.date_between(start_date="-1y", end_date="today").isoformat(),
            "claim_status": rng.choice(["Open", "Closed", "Subrogation", "Litigation"]),
            "coverage": rng.choice(COVERAGE_TYPES),
            "claimant_name": fake.name(),
            "injury_type": rng.choice([
                "Slip and Fall", "Auto Collision", "Property Damage",
                "Equipment Failure", "Repetitive Strain", "Chemical Exposure",
                None,
            ]),
            "paid_amount": paid,
            "reserved_amount": reserved,
            "total_incurred": paid + reserved,
            "state": rng.choice(US_STATES),
            "adjuster": fake.name(),
        })

    with open(filepath, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"✓ summit/{filename} ({len(records)} rows)")


def generate_beacon_excel():
    """Beacon Mutual — premium bordereau as Excel."""
    carrier = "Beacon Mutual"
    filename = f"beacon_bordereau_{datetime.now().strftime('%Y%m')}.xlsx"
    filepath = os.path.join(OUTPUT_DIR, "beacon", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "Bordereau"

    headers = [
        "Reporting Month", "Agency #", "Insured Legal Name", "FEIN",
        "Policy Nbr", "WC Class Code", "State", "Payroll",
        "Manual Premium", "Experience Mod", "Modified Premium",
        "Schedule Credit", "Net Premium", "Eff", "Exp",
    ]
    ws.append(headers)

    for _ in range(rng.randint(150, 250)):
        payroll = round(rng.uniform(50000, 5000000), 2)
        manual_rate = rng.uniform(0.005, 0.15)
        manual_premium = round(payroll * manual_rate, 2)
        exp_mod = round(rng.uniform(0.60, 1.80), 2)
        modified = round(manual_premium * exp_mod, 2)
        schedule_credit = round(rng.uniform(-0.25, 0.10), 4)
        net = round(modified * (1 + schedule_credit), 2)
        eff = fake.date_between(start_date="-1y", end_date="+3m")

        ws.append([
            datetime.now().strftime("%Y-%m"),
            rng.choice(AGENCY_CODES),
            rng.choice(BUSINESS_NAMES),
            f"{rng.randint(10, 99)}-{rng.randint(1000000, 9999999)}",
            f"WC-{rng.randint(100000, 999999)}",
            rng.choice(["8810", "8742", "5183", "8018", "9014", "7380", "3632"]),
            rng.choice(US_STATES),
            payroll,
            manual_premium,
            exp_mod,
            modified,
            schedule_credit,
            net,
            eff.isoformat(),
            (eff + timedelta(days=365)).isoformat(),
        ])

    wb.save(filepath)
    print(f"✓ beacon/{filename} ({ws.max_row - 1} rows)")


def generate_patriot_csv():
    """Patriot Indemnity — endorsement log CSV."""
    carrier = "Patriot Indemnity"
    filename = f"patriot_endorsements_{datetime.now().strftime('%Y%m')}.csv"
    filepath = os.path.join(OUTPUT_DIR, "patriot", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    headers = [
        "EndorsementID", "PolicyNumber", "AgencyCode", "NamedInsured",
        "EndorsementType", "EffectiveDate", "Description",
        "PremiumChange", "NewTotalPremium",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _ in range(rng.randint(80, 150)):
            base_premium = round(rng.uniform(5000, 200000), 2)
            change = round(rng.uniform(-20000, 50000), 2)
            writer.writerow([
                f"END-{rng.randint(100000, 999999)}",
                f"PI-{rng.randint(100000, 999999)}",
                rng.choice(AGENCY_CODES),
                rng.choice(BUSINESS_NAMES),
                rng.choice([
                    "Additional Insured", "Address Change", "Coverage Increase",
                    "Coverage Decrease", "Vehicle Add", "Vehicle Remove",
                    "Driver Add", "Class Code Change",
                ]),
                fake.date_between(start_date="-6m", end_date="+1m").isoformat(),
                rng.choice([
                    "Added additional insured per certificate request",
                    "Updated mailing address",
                    "Increased BPP limit from 500K to 1M",
                    "Added 2024 Ford Transit to schedule",
                    "Removed sold vehicle from policy",
                    "Reclassified per payroll audit",
                ]),
                change,
                base_premium + change,
            ])

    print(f"✓ patriot/{filename} ({_count_csv_rows(filepath)} rows)")


def _count_csv_rows(filepath):
    """Count data rows in a CSV file (excludes header)."""
    with open(filepath) as f:
        return sum(1 for _ in f) - 1


def main():
    """Generate all carrier fixture files."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("── Generating Carrier files ──")
    generate_acme_csv()
    generate_liberty_csv()
    generate_summit_json()
    generate_beacon_excel()
    generate_patriot_csv()
    print(f"✓ Carrier fixtures written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
