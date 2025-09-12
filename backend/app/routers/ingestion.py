import pandas as pd
import psycopg2
from fastapi import APIRouter, UploadFile
from datetime import datetime

from ..config import DATABASE_CONFIG

router = APIRouter()

@router.post("/upload-csv/")
async def upload_csv(file: UploadFile, clerk_user_id: str):
    df = pd.read_csv(file.file)

    # Ensure datetime conversion
    df['dispatch_date'] = pd.to_datetime(df.get('dispatch_date'), errors='coerce')
    df['delivery_date'] = pd.to_datetime(df.get('delivery_date'), errors='coerce')

    # Compute lead_time_days & disruption_type if missing
    if 'lead_time_days' not in df.columns:
        df['lead_time_days'] = (df['delivery_date'] - df['dispatch_date']).dt.days
    if 'disruption_type' not in df.columns:
        df['disruption_type'] = df['delay_days'].apply(lambda x: "Delay" if pd.notnull(x) and x > 0 else "On-Time")

    df['user_id'] = clerk_user_id

    conn = psycopg2.connect(**DATABASE_CONFIG)
    cur = conn.cursor()

    # Insert each row safely (convert NaT -> None)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO shipments (
                user_id, shipment_id, origin, destination, dispatch_date, delivery_date,
                delay_days, disruption_type, risk_score, lead_time_days, route_risk_score,
                delay_severity, month, weekday, quarter, year
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row['user_id'],
            row.get('shipment_id'),
            row.get('origin'),
            row.get('destination'),
            row['dispatch_date'].to_pydatetime() if pd.notnull(row['dispatch_date']) else None,
            row['delivery_date'].to_pydatetime() if pd.notnull(row['delivery_date']) else None,
            row.get('delay_days'),
            row.get('disruption_type'),
            row.get('risk_score'),
            int(row['lead_time_days']) if pd.notnull(row['lead_time_days']) else None,
            row.get('route_risk_score'),
            row.get('delay_severity'),
            row.get('month'),
            row.get('weekday'),
            row.get('quarter'),
            row.get('year')
        ))

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "success", "rows_uploaded": len(df)}
