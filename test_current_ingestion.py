# test_fixed_ingestion.py
import sys
import os

# Add src to path so we can import your ingestion module
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from data.ingestion import ingest_csv_to_postgres
    
    csv_path = 'data/raw/sample_meter_data.csv'
    print(f"🔄 Testing FIXED ingestion script...")
    print(f"📂 CSV path: {csv_path}")
    
    # Run the fixed ingestion function
    ingest_csv_to_postgres(csv_path)
    print("✅ Fixed ingestion completed successfully!")
    
except Exception as e:
    print(f"❌ Fixed ingestion failed: {e}")
    import traceback
    traceback.print_exc()
    
    # Try the manual approach
    print("\n🔄 Trying manual insertion approach...")
    try:
        from data.ingestion_manual import ingest_csv_to_postgres
        ingest_csv_to_postgres(csv_path)
        print("✅ Manual ingestion completed successfully!")
    except Exception as e2:
        print(f"❌ Manual ingestion also failed: {e2}")