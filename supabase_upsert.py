import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

DB_URL = os.getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres")
OUT_DIR = Path(__file__).parent / "output"

def connect():
    return psycopg2.connect(DB_URL)

def upsert_properties(df, conn):
    if df.empty: return
    cols = ["property_id","owner_name","city","state","region","status"]
    vals = df[cols].drop_duplicates(subset=["property_id"]).values.tolist()
    sql = """
    INSERT INTO properties (property_id, owner_name, city, state, region, status)
    VALUES %s
    ON CONFLICT (property_id) DO UPDATE SET
      owner_name = EXCLUDED.owner_name,
      city = EXCLUDED.city,
      state = EXCLUDED.state,
      region = EXCLUDED.region,
      status = EXCLUDED.status;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    conn.commit()

def upsert_bookings(df, conn):
    if df.empty: return
    sql = """
    INSERT INTO bookings (property_id, month, num_reservations, occupied_days, gross_revenue)
    VALUES %s
    ON CONFLICT (property_id, month) DO UPDATE SET
      num_reservations = EXCLUDED.num_reservations,
      occupied_days = EXCLUDED.occupied_days,
      gross_revenue = EXCLUDED.gross_revenue;
    """
    vals = []
    for _, r in df.iterrows():
        vals.append((r["property_id"], r["month"], int(r.get("num_reservations") or 0),
                     int(r.get("occupied_days") or 0), float(r.get("gross_revenue") or 0.0)))
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    conn.commit()

def upsert_financials(df, conn):
    if df.empty: return
    sql = """
    INSERT INTO financials (property_id, month, platform_fee_pct, extra_cost, gross_revenue, net_revenue, margin_pct)
    VALUES %s
    ON CONFLICT (property_id, month) DO UPDATE SET
      platform_fee_pct = EXCLUDED.platform_fee_pct,
      extra_cost = EXCLUDED.extra_cost,
      gross_revenue = EXCLUDED.gross_revenue,
      net_revenue = EXCLUDED.net_revenue,
      margin_pct = EXCLUDED.margin_pct;
    """
    vals = []
    for _, r in df.iterrows():
        vals.append((r["property_id"], r["month"], float(r.get("platform_fee_pct") or 0.0),
                     float(r.get("extra_cost") or 0.0), float(r.get("gross_revenue") or 0.0),
                     float(r.get("net_revenue") or 0.0), float(r.get("margin_pct") or 0.0)))
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    conn.commit()

def upsert_feedbacks(df, conn):
    if df.empty: return
    sql = """
    INSERT INTO feedbacks (property_id, month, avg_rating, complaint_categories, summary_ai)
    VALUES %s
    ON CONFLICT (property_id, month) DO UPDATE SET
      avg_rating = EXCLUDED.avg_rating,
      complaint_categories = EXCLUDED.complaint_categories,
      summary_ai = EXCLUDED.summary_ai;
    """
    vals = []
    for _, r in df.iterrows():
        vals.append((r["property_id"], r["month"], float(r.get("avg_rating")) if r.get("avg_rating") is not None else None,
                     json.dumps(r.get("complaint_categories") or {}), r.get("summary_ai") or None))
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    conn.commit()

def main():
    conn = connect()
    # read consolidated csv
    cons_path = OUT_DIR / f"consolidated_{pd.Timestamp.now().strftime('%Y-%m')}.csv"
    if not cons_path.exists():
        # try generic
        files = list(OUT_DIR.glob("consolidated_*.csv"))
        cons_path = files[-1] if files else None
    if cons_path:
        df = pd.read_csv(cons_path)
        upsert_properties(df, conn)
        upsert_bookings(df, conn)
        upsert_financials(df, conn)
    fb_path = OUT_DIR / f"feedbacks_{pd.Timestamp.now().strftime('%Y-%m')}.csv"
    if not fb_path.exists():
        files = list(OUT_DIR.glob("feedbacks_*.csv"))
        fb_path = files[-1] if files else None
    if fb_path:
        df_fb = pd.read_csv(fb_path)
        upsert_feedbacks(df_fb, conn)
    conn.close()
    print("[INFO] Upsert concluído.")

if __name__ == "__main__":
    import json
    main()
