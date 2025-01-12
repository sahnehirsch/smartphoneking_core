import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

def diagnose_data():
    """Diagnose potential issues with data_for_api and prices tables"""
    print("\n=== Data Diagnosis Report ===")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Check prices table first
    prices_result = supabase.table('prices').select(
        'run_id', count='exact'
    ).eq('run_id', 'f4a119db-945b-4670-ab24-5937b59261ab').execute()
    
    print("=== Prices Table ===")
    print(f"Total prices for run f4a119db: {prices_result.count:,}")
    
    # Check valid prices (non-error)
    valid_prices = supabase.table('prices').select(
        'run_id', count='exact'
    ).eq('run_id', 'f4a119db-945b-4670-ab24-5937b59261ab').eq('price_error', False).execute()
    
    print(f"Valid prices (non-error) for run f4a119db: {valid_prices.count:,}")
    
    # Check data_for_api table
    api_data_result = supabase.table('data_for_api').select(
        'run_id', count='exact'
    ).eq('run_id', 'f4a119db-945b-4670-ab24-5937b59261ab').execute()
    
    print("\n=== Data For API Table ===")
    print(f"Total entries for run f4a119db: {api_data_result.count:,}")
    
    # Check for duplicate entries
    duplicate_check = supabase.table('data_for_api').select(
        'smartphone_id,retailer_id,price,count'
    ).eq('run_id', 'f4a119db-945b-4670-ab24-5937b59261ab').execute()
    
    print("\n=== Checking for Duplicates ===")
    # Process results to find duplicates
    entries = {}
    duplicates = 0
    for entry in duplicate_check.data:
        key = f"{entry['smartphone_id']}-{entry['retailer_id']}-{entry['price']}"
        if key in entries:
            duplicates += 1
        else:
            entries[key] = 1
            
    print(f"Found {duplicates:,} duplicate entries in data_for_api")
    
    # Check the delete operation in update_api_data.py
    print("\n=== Analyzing Previous Runs ===")
    runs_result = supabase.table('data_for_api').select(
        'run_id'
    ).neq('run_id', 'f4a119db-945b-4670-ab24-5937b59261ab').execute()
    
    other_runs = set(r['run_id'] for r in runs_result.data)
    print(f"Number of other run_ids still present: {len(other_runs)}")
    if other_runs:
        print("Other run_ids found (should be empty):")
        for run in other_runs:
            print(f"- {run}")

if __name__ == '__main__':
    diagnose_data()
