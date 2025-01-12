import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
from collections import defaultdict
import sys

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

def get_table_info(table_name: str) -> dict:
    """Get table structure information"""
    try:
        # Using RPC call to get table information
        result = supabase.rpc(
            'get_table_info',
            {'table_name': table_name}
        ).execute()
        return result.data if result.data else {}
    except Exception as e:
        print(f"Error getting table info for {table_name}: {e}", file=sys.stderr)
        return {}

def diagnose_data():
    """Diagnose potential issues with data_for_api and prices tables"""
    print("\n=== Data Diagnosis Report ===")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    run_id = 'f4a119db-945b-4670-ab24-5937b59261ab'

    # Check prices table first
    try:
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
    except Exception as e:
        print(f"Error querying prices table: {e}", file=sys.stderr)

    # Check data_for_api table
    try:
        api_data_result = supabase.table('data_for_api').select(
            'run_id', count='exact'
        ).eq('run_id', run_id).execute()
        
        print("\n=== Data For API Table ===")
        print(f"Total entries for run {run_id}: {api_data_result.count:,}")
    except Exception as e:
        print(f"Error querying data_for_api table: {e}", file=sys.stderr)
    
    # Get sample of price_ids from both tables
    print("\n=== Price ID Analysis ===")
    try:
        # Get ALL price_ids from prices table
        print("\nGetting price_ids from prices table...")
        all_prices_ids = supabase.table('prices').select(
            'price_id'
        ).eq('run_id', run_id).eq('price_error', False).execute()
        
        print("Getting price_ids from data_for_api table...")
        all_api_ids = supabase.table('data_for_api').select(
            'price_id'
        ).eq('run_id', run_id).execute()
        
        # Sample display
        print("\nSample price_ids from prices table (first 5):")
        for p in all_prices_ids.data[:5]:
            print(f"  {p['price_id']}")
            
        print("\nSample price_ids from data_for_api table (first 5):")
        for p in all_api_ids.data[:5]:
            print(f"  {p['price_id']}")
        
        # Full analysis
        prices_set = set(p['price_id'] for p in all_prices_ids.data)
        api_set = set(p['price_id'] for p in all_api_ids.data)
        
        print(f"\nNumber of unique price_ids in prices table: {len(prices_set):,}")
        print(f"Number of unique price_ids in data_for_api table: {len(api_set):,}")
        
        overlap = prices_set.intersection(api_set)
        print(f"Number of overlapping price_ids: {len(overlap):,}")
        
        # Distribution analysis
        if len(prices_set) > 0:
            prices_list = sorted(list(prices_set))
            print(f"\nPrices table price_id range: {min(prices_list):,} to {max(prices_list):,}")
        
        if len(api_set) > 0:
            api_list = sorted(list(api_set))
            print(f"Data_for_api table price_id range: {min(api_list):,} to {max(api_list):,}")
            
    except Exception as e:
        print(f"Error during price_id analysis: {e}", file=sys.stderr)
    
    # Check for duplicate entries
    print("\n=== Checking for Duplicates ===")
    try:
        # Get all entries for duplicate checking
        all_entries = supabase.table('data_for_api').select(
            'smartphone_id,retailer_id,price,price_id,run_id'
        ).eq('run_id', run_id).execute()
        
        # Process results to find duplicates
        entry_counts = defaultdict(list)  # Changed to list to store price_ids
        for entry in all_entries.data:
            key = f"{entry['smartphone_id']}-{entry['retailer_id']}-{entry['price']}"
            entry_counts[key].append(entry['price_id'])
        
        duplicates = {k: v for k, v in entry_counts.items() if len(v) > 1}
        print(f"Found {len(duplicates)} keys with duplicates")
        if duplicates:
            print("\nTop 5 duplicated entries:")
            for key, price_ids in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
                print(f"Key {key}: {len(price_ids)} occurrences (price_ids: {price_ids})")
    except Exception as e:
        print(f"Error checking for duplicates: {e}", file=sys.stderr)
    
    # Check the delete operation in update_api_data.py
    print("\n=== Analyzing Previous Runs ===")
    try:
        runs_result = supabase.table('data_for_api').select(
            'run_id'
        ).neq('run_id', run_id).execute()
        
        other_runs = set(r['run_id'] for r in runs_result.data)
        print(f"Number of other run_ids still present: {len(other_runs)}")
        if other_runs:
            print("Other run_ids found (should be empty):")
            for run in sorted(other_runs)[:5]:
                print(f"- {run}")
            if len(other_runs) > 5:
                print(f"... and {len(other_runs) - 5} more")
    except Exception as e:
        print(f"Error checking previous runs: {e}", file=sys.stderr)

if __name__ == '__main__':
    diagnose_data()
