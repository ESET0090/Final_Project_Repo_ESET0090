# check_csv.py
import pandas as pd
import os

def check_csv_file():
    csv_path = 'data/raw/sample_meter_data.csv'
    print(f"📂 Checking CSV file: {csv_path}")
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        
        # Check what files exist
        raw_dir = 'data/raw'
        if os.path.exists(raw_dir):
            files = os.listdir(raw_dir)
            print(f"📁 Files in {raw_dir}:")
            for file in files:
                print(f"   - {file}")
        return False
    
    try:
        # Try to read the CSV
        df = pd.read_csv(csv_path)
        print("✅ CSV file loaded successfully!")
        print(f"📊 Shape: {df.shape} (rows: {df.shape[0]}, columns: {df.shape[1]})")
        print(f"🏷️ Columns: {df.columns.tolist()}")
        
        # Check data types
        print(f"🔧 Data types:")
        for col in df.columns:
            print(f"   - {col}: {df[col].dtype}")
        
        # Check for timestamp column (might be named differently)
        timestamp_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower() or 'ts' in col]
        if timestamp_cols:
            print(f"📅 Timestamp column found: {timestamp_cols[0]}")
            print(f"   Date range: {df[timestamp_cols[0]].min()} to {df[timestamp_cols[0]].max()}")
        else:
            print("⚠️  No obvious timestamp column found")
        
        # Check meter_id column
        meter_cols = [col for col in df.columns if 'meter' in col.lower() or 'id' in col.lower()]
        if meter_cols:
            print(f"🔢 Meter ID column found: {meter_cols[0]}")
            print(f"   Unique meters: {df[meter_cols[0]].nunique()}")
        
        print(f"\n📈 First 3 rows:")
        print(df.head(3))
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return False

if __name__ == "__main__":
    check_csv_file()