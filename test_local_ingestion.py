# test_local_ingestion.py
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from data.ingestion import ingest_csv_to_postgres
    
    csv_path = 'data/raw/sample_meter_data.csv'
    print(f"🔄 Testing LOCAL ingestion script...")
    print(f"📂 CSV path: {csv_path}")
    
    # Run the local ingestion function
    ingest_csv_to_postgres(csv_path)
    print("✅ Local ingestion completed successfully!")
    
except Exception as e:
    print(f"❌ Local ingestion failed: {e}")
    import traceback
    traceback.print_exc()