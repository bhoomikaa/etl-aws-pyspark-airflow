# scripts/generate_data.py
# Bank-like synthetic data generator.
# Writes newline-delimited JSON (JSONL) partitioned by day and source:
# data/raw/day=YYYY-MM-DD/source=<payments|billing|crm|erp|support>/part-00000.json

import os, json, uuid, random, argparse, math
from datetime import datetime, timedelta
from pathlib import Path

# Five systems to mirror banking domains (keep names consistent with your plan)
SOURCES = ["payments", "billing", "crm", "erp", "support"]

# Common constants
CURRENCIES = ["USD"]  # keep simple for now
COUNTRIES = ["US","CA","GB","DE","FR","IN","AU","SG"]
CARD_NETWORKS = ["VISA","MASTERCARD","AMEX"]
POS_ENTRY_MODES = ["chip","contactless","magstripe","ecommerce"]
PAYMENT_CHANNELS = ["ach_credit","ach_debit","wire_transfer","card_auth","card_settlement","zelle"]
MCCS = [
    {"mcc":"5411","category":"Grocery"},
    {"mcc":"5812","category":"Restaurant"},
    {"mcc":"5732","category":"Electronics"},
    {"mcc":"4814","category":"Telecom"},
    {"mcc":"5999","category":"Specialty Retail"},
    {"mcc":"6011","category":"ATM"}
]
SUPPORT_CATEGORIES = ["chargeback","card_stolen","login_issue","payment_failed","address_change","refund_request"]
SUPPORT_PRIORITIES = ["low","medium","high","urgent"]
CRM_EVENTS = ["kyc_verified","kyc_pending","kyc_refresh_due","address_update","phone_update","account_locked","login"]
ERP_JOURNALS = ["payments_settlement","card_interchange","fees_accrual","refunds","chargebacks","revenue_recognition"]
GL_ACCOUNTS = ["1000-Cash","1100-Receivables","2000-DepositsLiability","4000-InterchangeRevenue","5000-FeesExpense","5100-Refunds"]

random.seed(42)  # deterministic-ish runs for repeatability

def now_utc():
    return datetime.utcnow()

def rand_ts_for_day(day_str: str):
    day_start = datetime.strptime(day_str, "%Y-%m-%d")
    return day_start + timedelta(seconds=random.randint(0, 86399))

def base_record(day_str: str, source: str):
    ts = rand_ts_for_day(day_str)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 500000),
        "currency": random.choice(CURRENCIES),
        "source_system": source,
        "timestamp": ts.isoformat() + "Z"
    }

def rand_amount(lo=1.0, hi=500.0, purchase=False):
    # Purchases skew smaller; wires skew larger
    if purchase:
        return round(random.triangular(5.0, 60.0, 300.0), 2)
    return round(random.uniform(lo, hi), 2)

def nine_digits():
    return "".join(str(random.randint(0,9)) for _ in range(9))

def pan_last4():
    return "".join(str(random.randint(0,9)) for _ in range(4))

def swift_bic():
    # Fake-ish BIC: 4 letters bank + 2 letters country + 2 alnum
    return "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(4)) + \
           random.choice(["US","GB","DE","FR","CA","IN","AU","SG"]) + \
           "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(2))

def mask_iban():
    # Just a mask-like string; not a real IBAN
    return "****" + "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(10))

def merchant():
    m = random.choice(MCCS)
    return {
        "merchant_id": str(uuid.uuid4())[:8],
        "merchant_name": random.choice(["Acme Stores","MetroMart","Cafe Aurora","TechHub","TelcoMax","Global ATM"]),
        "merchant_country": random.choice(COUNTRIES),
        "mcc": m["mcc"],
        "category": m["category"]
    }

def gen_payments(day_str: str):
    base = base_record(day_str, "payments")
    channel = random.choices(PAYMENT_CHANNELS, weights=[20,20,10,25,20,5])[0]
    status = random.choices(["posted","pending","failed"], weights=[80,17,3])[0]

    if channel in ("card_auth","card_settlement"):
        m = merchant()
        event_type = channel
        amt = rand_amount(purchase=True)
        rec = {
            **base,
            "event_type": event_type,
            "amount": amt,
            "card_network": random.choice(CARD_NETWORKS),
            "pan_last4": pan_last4(),
            "card_present": random.choice([True, False]),
            "pos_entry_mode": random.choice(POS_ENTRY_MODES),
            **m,
            "approval_code": str(uuid.uuid4())[:6] if status != "failed" else None,
            "decline_reason": None if status != "failed" else random.choice(["insufficient_funds","suspected_fraud","do_not_honor"]),
            "status": status
        }
        return rec

    if channel in ("ach_credit","ach_debit"):
        event_type = channel
        amt = rand_amount(5, 2000)
        rec = {
            **base,
            "event_type": event_type,
            "amount": amt if channel == "ach_credit" else -amt,
            "routing_number": nine_digits(),
            "account_last4": pan_last4(),
            "counterparty_name": random.choice(["Payroll Inc","Utility Co","John Smith","Jane Doe"]),
            "trace_number": str(uuid.uuid4())[:12],
            "status": status
        }
        return rec

    if channel == "wire_transfer":
        amt = rand_amount(100, 10000)
        rec = {
            **base,
            "event_type": "wire_transfer",
            "amount": random.choice([amt, -amt]),  # incoming/outgoing
            "swift_bic": swift_bic(),
            "iban_masked": mask_iban(),
            "is_international": random.choice([True, False]),
            "fees": round(amt * 0.003, 2),
            "status": status
        }
        return rec

    # zelle or fallback
    amt = rand_amount(5, 500)
    rec = {
        **base,
        "event_type": "zelle_payment",
        "amount": random.choice([amt, -amt]),
        "counterparty_alias": random.choice(["+1-202-555-0101","friend@example.com","$roommate"]),
        "status": status
    }
    return rec

def gen_billing(day_str: str):
    base = base_record(day_str, "billing")
    status = random.choices(["open","paid","overdue","refunded"], weights=[40,45,10,5])[0]
    invoice_id = "INV-" + str(uuid.uuid4())[:8]
    issued = rand_ts_for_day(day_str)
    due = issued + timedelta(days=random.choice([7,14,30]))
    amt = rand_amount(20, 2000)
    paid = amt if status in ("paid","refunded") else round(amt * random.choice([0,0.25,0.5,0.75]), 2)

    event_type = random.choice(["invoice_issued","invoice_paid","refund_issued"])
    return {
        **base,
        "event_type": event_type,
        "invoice_id": invoice_id,
        "invoice_date": issued.isoformat() + "Z",
        "due_date": due.isoformat() + "Z",
        "status": status,
        "amount": amt if event_type != "refund_issued" else -paid,
        "amount_due": max(0.0, amt - paid),
        "amount_paid": paid,
        "tax_rate": random.choice([0.0,0.05,0.07,0.1]),
        "line_count": random.randint(1,5)
    }

def gen_crm(day_str: str):
    base = base_record(day_str, "crm")
    event_type = random.choices(CRM_EVENTS, weights=[25,10,5,20,15,5,20])[0]
    risk = random.randint(1, 99)
    return {
        **base,
        "event_type": event_type,
        "amount": 0.0,
        "kyc_status": random.choice(["pending","verified","refresh_due","blocked"]),
        "risk_score": risk,
        "pep_flag": random.choice([False, False, False, True]),  # rare
        "ip": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
        "country": random.choice(COUNTRIES)
    }

def gen_erp(day_str: str):
    base = base_record(day_str, "erp")
    journal = random.choice(ERP_JOURNALS)
    acct = random.choice(GL_ACCOUNTS)
    # For a single record, choose debit or credit side; amount is positive magnitude.
    amt = rand_amount(5, 5000)
    side = random.choice(["debit","credit"])
    return {
        **base,
        "event_type": "gl_posting",
        "journal_type": journal,
        "gl_account": acct,
        "debit": amt if side == "debit" else 0.0,
        "credit": amt if side == "credit" else 0.0,
        "amount": amt,  # magnitude for convenience
        "entity": random.choice(["HQ","NYC","SFO","LON"]),
        "posted_by": random.choice(["batch_job","integration","analyst"])
    }

def gen_support(day_str: str):
    base = base_record(day_str, "support")
    cat = random.choice(SUPPORT_CATEGORIES)
    pri = random.choices(SUPPORT_PRIORITIES, weights=[50,30,15,5])[0]
    status = random.choices(["open","in_progress","resolved","closed"], weights=[20,30,35,15])[0]
    return {
        **base,
        "event_type": "ticket_" + ("closed" if status in ["resolved","closed"] else "opened"),
        "amount": 0.0,
        "ticket_id": "TCK-" + str(uuid.uuid4())[:8],
        "category": cat,
        "priority": pri,
        "status": status,
        "channel": random.choice(["phone","email","in_app","chat"])
    }

GEN_BY_SOURCE = {
    "payments": gen_payments,
    "billing": gen_billing,
    "crm": gen_crm,
    "erp": gen_erp,
    "support": gen_support,
}

def write_partition(day_str: str, source: str, out_dir: Path, n_events: int, events_per_file: int):
    base = out_dir / f"day={day_str}" / f"source={source}"
    base.mkdir(parents=True, exist_ok=True)
    n_files = max(1, math.ceil(n_events / events_per_file))
    remaining = n_events
    for i in range(n_files):
        count = min(events_per_file, remaining)
        fp = base / f"part-{i:05d}.json"
        with fp.open("w", encoding="utf-8") as f:
            for _ in range(count):
                rec = GEN_BY_SOURCE[source](day_str)
                # Ensure required common fields exist for downstream
                if "amount" not in rec:
                    rec["amount"] = 0.0
                if "event_type" not in rec:
                    rec["event_type"] = "event"
                f.write(json.dumps(rec) + "\n")
        remaining -= count
        print(f"Wrote {count} events -> {fp}")
    return base

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw", help="Output root folder")
    p.add_argument("--day", default=datetime.utcnow().strftime("%Y-%m-%d"), help="End day (YYYY-MM-DD, UTC)")
    p.add_argument("--days", type=int, default=1, help="Number of days back, including --day")
    p.add_argument("--total-events", type=int, default=60000, help="Total events per day across all sources")
    p.add_argument("--events-per-file", type=int, default=60000, help="Events per file (use big to keep S3 PUTs low)")
    args = p.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    for d in range(args.days):
        day_str = (datetime.strptime(args.day, "%Y-%m-%d") - timedelta(days=d)).strftime("%Y-%m-%d")
        per_source = args.total_events // len(SOURCES)
        remainder = args.total_events - per_source * len(SOURCES)
        for idx, source in enumerate(SOURCES):
            n = per_source + (1 if idx < remainder else 0)
            write_partition(day_str, source, out_dir, n, args.events_per_file)
        print(f"✅ Generated ~{args.total_events} events for {day_str} under {out_dir}")
