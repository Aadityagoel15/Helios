import pandas as pd
import asyncpg
from fastapi import APIRouter, UploadFile, HTTPException
from datetime import datetime
from ..config import DATABASE_CONFIG

router = APIRouter()

REQUIRED_COLUMNS = ['shipment_id', 'origin', 'destination', 'dispatch_date', 'delivery_date']

@router.post("/upload-csv/")
async def upload_csv(file: UploadFile, clerk_user_id: str):
    try:
        df = pd.read_csv(file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")

    # Validate required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing_cols}")

    # Ensure datetime conversion
    df['dispatch_date'] = pd.to_datetime(df['dispatch_date'], errors='coerce')
    df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')

    # Compute lead_time_days & disruption_type if missing
    if 'lead_time_days' not in df.columns:
        df['lead_time_days'] = (df['delivery_date'] - df['dispatch_date']).dt.days
    if 'disruption_type' not in df.columns:
        df['disruption_type'] = df['delay_days'].apply(
            lambda x: "Delay" if pd.notnull(x) and x > 0 else "On-Time"
        )

    df['user_id'] = clerk_user_id

    # Prepare batch insert data
    data_to_insert = []
    for _, row in df.iterrows():
        data_to_insert.append((
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

    # Async DB insert
    try:
        conn = await asyncpg.connect(**DATABASE_CONFIG)
        insert_query = """
            INSERT INTO shipments (
                user_id, shipment_id, origin, destination, dispatch_date, delivery_date,
                delay_days, disruption_type, risk_score, lead_time_days, route_risk_score,
                delay_severity, month, weekday, quarter, year
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
            )
        """
        # Batch insert using executemany
        await conn.executemany(insert_query, data_to_insert)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB insert error: {str(e)}")
    finally:
        await conn.close()

    return {"status": "success", "rows_uploaded": len(df)}
