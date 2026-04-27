import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="NexaFlow Predictive Analytics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@postgres:5432/finventory_db",
)
engine = create_engine(DATABASE_URL)

@app.get("/forecast/{product_id}")
def get_forecast(product_id: int):
    try:
        with engine.connect() as conn:
            # Get current stock
            prod = conn.execute(text("SELECT name, stock FROM products WHERE id = :pid"), {"pid": product_id}).fetchone()
            if not prod:
                raise HTTPException(status_code=404, detail="Product not found")
                
            # Calculate daily sales rate
            tx_count = conn.execute(
                text("SELECT COALESCE(SUM(quantity), 0) as total FROM transactions WHERE product_id = :pid AND category = 'sale'"),
                {"pid": product_id}
            ).fetchone()[0]
            
            # Simple assumption: historical sales over a fixed 30-day window
            daily_rate = max(tx_count / 30.0, 0.5) # floor at 0.5/day to avoid infinity
            days_left = int(prod.stock / daily_rate)
            
            return {
                "product": prod.name,
                "current_stock": prod.stock,
                "daily_sales_rate": round(daily_rate, 2),
                "days_left": days_left,
                "message": f"Stock will run out in {days_left} days"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"service": "Predictive Analytics", "status": "running"}
