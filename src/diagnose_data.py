import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
from collections import defaultdict

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

    run_id = 'f4a119db-945b-4670-ab24-5937b59261ab'

    # Check prices table first
    prices_result = supabase.table('prices').select(
        'run_id', count='exact'
    ).eq('run_id', run_id).execute()
    
    print("=== Prices Table ===")
    print(f"Total prices for run {run_id}: {prices_result.count:,}")
    
    # Check valid prices (non-error)
    valid_prices = supabase.table('prices').select(
        'run_id', count='exact'
    ).eq('run_id', run_id).eq('price_error', False).execute()
    
    print(f"Valid prices (non-error) for run {run_id}: {valid_prices.count:,}")
    
    # Check data_for_api table
    api_data_result = supabase.table('data_for_api').select(
        'run_id', count='exact'
    ).eq('run_id', run_id).execute()
    
    print("\n=== Data For API Table ===")
    print(f"Total entries for run {run_id}: {api_data_result.count:,}")
    
    # Check for duplicate entries
    print("\n=== Checking for Duplicates ===")
    # Get all entries for duplicate checking
    all_entries = supabase.table('data_for_api').select(
        'smartphone_id,retailer_id,price,run_id'
    ).eq('run_id', run_id).execute()
    
    # Process results to find duplicates
    entry_counts = defaultdict(int)
    for entry in all_entries.data:
        key = f"{entry['smartphone_id']}-{entry['retailer_id']}-{entry['price']}"
        entry_counts[key] += 1
    
    duplicates = {k: v for k, v in entry_counts.items() if v > 1}
    print(f"Found {len(duplicates)} keys with duplicates")
    if duplicates:
        print("\nTop 5 duplicated entries:")
        for key, count in sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"Key {key}: {count} occurrences")
    
    # Check the delete operation in update_api_data.py
    print("\n=== Analyzing Previous Runs ===")
    runs_result = supabase.table('data_for_api').select(
        'run_id'
    ).neq('run_id', run_id).execute()
    
    other_runs = set(r['run_id'] for r in runs_result.data)
    print(f"Number of other run_ids still present: {len(other_runs)}")
    if other_runs:
        print("Other run_ids found (should be empty):")
        for run in sorted(other_runs)[:5]:  # Show only first 5 to avoid overwhelming output
            print(f"- {run}")
        if len(other_runs) > 5:
            print(f"... and {len(other_runs) - 5} more")

if __name__ == '__main__':
    diagnose_data()
