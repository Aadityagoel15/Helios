-- Table to store shipments linked to a user
CREATE TABLE shipments (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,           -- Clerk user ID
    shipment_id TEXT,
    origin VARCHAR(100),
    destination VARCHAR(100),
    dispatch_date TIMESTAMP,
    delivery_date DATE,
    delay_days INT,
    disruption_type TEXT,
    risk_score NUMERIC,
    lead_time_days INT,
    route_risk_score NUMERIC,
    delay_severity TEXT,
    month INT,
    weekday INT,
    quarter INT,
    year INT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
