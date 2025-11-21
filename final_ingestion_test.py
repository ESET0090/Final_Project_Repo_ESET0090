# final_ingestion_test.py
import pandas as pd
import psycopg2

def test_ingestion():
    print("🔄 Testing Data Ingestion...")
    
    # Read CSV
    df = pd.read_csv('data/raw/sample_meter_data.csv', parse_dates=["ts"])
    print(f"✅ Read {len(df)} rows from CSV")
    print(f"📋 Columns: {df.columns.tolist()}")
    
    try:
        # Connect directly using psycopg2 (no SQLAlchemy issues)
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="ml_user",
            password="ml_pass",
            database="ml_db"
        )
        cursor = conn.cursor()
        
        # Check current count
        cursor.execute("SELECT COUNT(*) FROM raw_meter_data")
        current_count = cursor.fetchone()[0]
        print(f"📊 Current rows in raw_meter_data: {current_count}")
        
        # Insert data in batches
        print("💾 Inserting data...")
        inserted_count = 0
        
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO raw_meter_data (meter_id, ts, load_kwh) VALUES (%s, %s, %s)",
                (row['meter_id'], row['ts'], row['load_kwh'])
            )
            inserted_count += 1
            
            # Show progress every 100 rows
            if inserted_count % 100 == 0:
                print(f"   ✅ Inserted {inserted_count} rows...")
        
        # Commit the transaction
        conn.commit()
        
        # Check new count
        cursor.execute("SELECT COUNT(*) FROM raw_meter_data")
        new_count = cursor.fetchone()[0]
        print(f"📊 New rows in raw_meter_data: {new_count}")
        print(f"📈 Rows added: {new_count - current_count}")
        
        cursor.close()
        conn.close()
        
        print("🎉 Data ingestion completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        return False

if __name__ == "__main__":
    success = test_ingestion()
    if success:
        print("\n✅ SUCCESS! Your data pipeline is working!")
        print("🔗 Next: Test your Airflow DAG at http://localhost:8080")
    else:
        print("\n❌ FAILED! Check the error above.")