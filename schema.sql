CREATE TABLE IF NOT EXISTS properties (
  property_id VARCHAR PRIMARY KEY,
  owner_name VARCHAR,
  city VARCHAR,
  state VARCHAR,
  region VARCHAR,
  status VARCHAR,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bookings (
  id SERIAL PRIMARY KEY,
  property_id VARCHAR REFERENCES properties(property_id),
  month DATE,
  num_reservations INT,
  occupied_days INT,
  gross_revenue NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS financials (
  id SERIAL PRIMARY KEY,
  property_id VARCHAR REFERENCES properties(property_id),
  month DATE,
  platform_fee_pct NUMERIC,
  extra_cost NUMERIC,
  gross_revenue NUMERIC,
  net_revenue NUMERIC,
  margin_pct NUMERIC,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS feedbacks (
  id SERIAL PRIMARY KEY,
  property_id VARCHAR REFERENCES properties(property_id),
  month DATE,
  avg_rating NUMERIC,
  complaint_categories JSONB,
  summary_ai TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Consolidated view for BI / chatbot queries
CREATE OR REPLACE VIEW monthly_consolidated AS
SELECT
  p.property_id,
  p.owner_name,
  p.city,
  p.state,
  p.region,
  coalesce(b.month, f.month, fb.month) AS month,
  b.num_reservations,
  b.occupied_days,
  coalesce(f.gross_revenue, b.gross_revenue) AS gross_revenue,
  f.platform_fee_pct,
  f.extra_cost,
  f.net_revenue,
  f.margin_pct,
  fb.avg_rating,
  fb.complaint_categories,
  fb.summary_ai
FROM properties p
LEFT JOIN bookings b ON p.property_id = b.property_id
LEFT JOIN financials f ON p.property_id = f.property_id AND b.month = f.month
LEFT JOIN feedbacks fb ON p.property_id = fb.property_id AND b.month = fb.month;
