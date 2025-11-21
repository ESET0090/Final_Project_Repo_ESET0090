# test_connection.py
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

def test_postgres_connection():
    print("🔍 Testing PostgreSQL Connection...")
    
    # Method 1: Using SQLAlchemy with proper connection
    try:
        engine = create_engine('postgresql+psycopg2://ml_user:ml_pass@localhost:5432/ml_db')
        
        with engine.connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            print(f"✅ Basic connection test: {test_value}")
            
            # Get table list
            result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
            tables = [row[0] for row in result]
            print(f"📊 Tables in database: {len(tables)} tables found")
            
            # Check if our main tables exist
            target_tables = ['raw_meter_data', 'features', 'model_metrics', 'forecast_results']
            for table in target_tables:
                if table in tables:
                    print(f"   ✅ {table} exists")
                else:
                    print(f"   ❌ {table} missing")
            
            # Check data in raw_meter_data
            if 'raw_meter_data' in tables:
                print("\n📈 Checking data in raw_meter_data...")
                result = conn.execute(text("SELECT COUNT(*) as count FROM raw_meter_data"))
                count = result.scalar()
                print(f"   📊 Total rows: {count}")
                
                if count > 0:
                    result = conn.execute(text("SELECT * FROM raw_meter_data LIMIT 3"))
                    rows = result.fetchall()
                    print(f"   🔍 Sample rows:")
                    for row in rows:
                        print(f"      {row}")
                else:
                    print("   ℹ️  Table is empty - no data ingested yet")
                    
    except Exception as e:
        print(f"❌ SQLAlchemy connection failed: {e}")
        return False
    
    # Method 2: Direct psycopg2 connection
    try:
        print("\n🔍 Testing direct psycopg2 connection...")
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="ml_user",
            password="ml_pass",
            database="ml_db"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_meter_data")
        count = cursor.fetchone()[0]
        print(f"✅ Direct connection works - raw_meter_data has {count} rows")
        conn.close()
    except Exception as e:
        print(f"❌ Direct connection failed: {e}")
    
    return True

if __name__ == "__main__":
    test_postgres_connection()