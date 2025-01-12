import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
from collections import defaultdict
import sys
from typing import List, Dict, Set, Tuple

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

def get_all_records(table: str, select_query: str, run_id: str, page_size: int = 1000, 
                   extra_conditions: Dict = None) -> List[Dict]:
    """Get all records from a table using pagination"""
    all_records = []
    start = 0
    
    # First get total count
    try:
        count_query = supabase.table(table).select('*', count='exact').eq('run_id', run_id)
        if extra_conditions:
            for key, value in extra_conditions.items():
                count_query = count_query.eq(key, value)
        count_result = count_query.execute()
        total_count = count_result.count
        print(f"Total records to fetch: {total_count:,}")
    except Exception as e:
        print(f"Error getting total count: {e}", file=sys.stderr)
        return all_records

    while True:
        try:
            print(f"Fetching records {start:,} to {start + page_size - 1:,}...")
            query = supabase.table(table).select(select_query).eq('run_id', run_id)
            
            # Add any extra conditions
            if extra_conditions:
                for key, value in extra_conditions.items():
                    query = query.eq(key, value)
            
            # Add pagination using offset and limit instead of range
            result = query.limit(page_size).offset(start).execute()
            
            if not result.data:
                break
                
            records_count = len(result.data)
            all_records.extend(result.data)
            print(f"Retrieved {records_count:,} records in this batch")
            print(f"Total records retrieved so far: {len(all_records):,}")
            
            if records_count < page_size or len(all_records) >= total_count:
                break
                
            start += page_size
            
        except Exception as e:
            print(f"Error retrieving records at offset {start}: {e}", file=sys.stderr)
            # Don't break, try next batch
            start += page_size
            
    if len(all_records) < total_count:
        print(f"Warning: Only retrieved {len(all_records):,} records out of {total_count:,}", file=sys.stderr)
    
    return all_records

def analyze_sequential_patterns(price_ids: List[int]) -> Dict:
    """Analyze patterns in sequential price_ids"""
    if not price_ids:
        return {}
        
    sorted_ids = sorted(price_ids)
    sequences = []
    current_seq = [sorted_ids[0]]
    
    for i in range(1, len(sorted_ids)):
        if sorted_ids[i] == sorted_ids[i-1] + 1:
            current_seq.append(sorted_ids[i])
        else:
            if len(current_seq) > 1:
                sequences.append(current_seq)
            current_seq = [sorted_ids[i]]
    
    if len(current_seq) > 1:
        sequences.append(current_seq)
        
    return {
        'total_sequences': len(sequences),
        'longest_sequence': max((len(s) for s in sequences), default=0),
        'sequences': sequences[:5]  # Show only first 5 sequences
    }

def analyze_duplicates(data: List[Dict]) -> None:
    """Analyze duplicate entries in the data"""
    print("\n=== Checking for Duplicates ===")
    print("Getting all entries for duplicate analysis...")
    
    # Group by composite key
    duplicates = defaultdict(list)
    for item in data:
        key = f"{item['smartphone_id']}-{item['retailer_id']}-{item['price']}"
        duplicates[key].append(item['price_id'])
    
    # Filter for keys with more than one entry
    duplicate_keys = {k: v for k, v in duplicates.items() if len(v) > 1}
    
    print(f"\nFound {len(duplicate_keys)} keys with duplicates\n")
    if not duplicate_keys:
        return
    
    print("Top 5 duplicated entries:\n")
    for key in sorted(duplicate_keys.keys(), key=lambda k: len(duplicate_keys[k]), reverse=True)[:5]:
        price_ids = duplicate_keys[key]
        print(f"Key {key}:")
        print(f"  {len(price_ids)} occurrences")
        print(f"  Price IDs: {price_ids}")
        
        # Analyze sequential patterns
        sequences = []
        current_seq = [price_ids[0]]
        for i in range(1, len(price_ids)):
            if price_ids[i] == price_ids[i-1] + 1:
                current_seq.append(price_ids[i])
            else:
                if len(current_seq) > 1:
                    sequences.append(current_seq[:])
                current_seq = [price_ids[i]]
        if len(current_seq) > 1:
            sequences.append(current_seq)
            
        print("  Sequential patterns found:")
        print(f"    Number of sequences: {len(sequences)}")
        print(f"    Longest sequence: {max((len(s) for s in sequences), default=0)} IDs")
        print(f"    Sample sequences: {sequences[:5]}\n")

def get_latest_run_id() -> str:
    """Get the latest run_id from prices table"""
    try:
        result = supabase.table('prices').select(
            'run_id,date_recorded'
        ).order('date_recorded', desc=True).limit(1).execute()
        
        if not result.data:
            print("No runs found in prices table", file=sys.stderr)
            return None
            
        run_id = result.data[0]['run_id']
        print(f"Using latest run_id: {run_id} (recorded at: {result.data[0]['date_recorded']})")
        return run_id
    except Exception as e:
        print(f"Error getting latest run_id: {e}", file=sys.stderr)
        return None

def diagnose_data():
    """Diagnose potential issues with data_for_api and prices tables"""
    print("\n=== Data Diagnosis Report ===")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    run_id = get_latest_run_id()
    if not run_id:
        print("Could not get latest run_id", file=sys.stderr)
        return

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
    
    # Get price_ids from both tables with pagination
    print("\n=== Price ID Analysis ===")
    try:
        print("\nGetting price_ids from prices table...")
        prices_records = get_all_records('prices', 'price_id', run_id, 
                                       extra_conditions={'price_error': False})
        
        print("\nGetting price_ids from data_for_api table...")
        api_records = get_all_records('data_for_api', 'price_id', run_id)
        
        # Sample display
        print("\nSample price_ids from prices table (first 5):")
        for p in prices_records[:5]:
            print(f"  {p['price_id']}")
            
        print("\nSample price_ids from data_for_api table (first 5):")
        for p in api_records[:5]:
            print(f"  {p['price_id']}")
        
        # Full analysis
        prices_set = set(p['price_id'] for p in prices_records)
        api_set = set(p['price_id'] for p in api_records)
        
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
    
    # Check for duplicate entries with enhanced analysis
    print("\n=== Checking for Duplicates ===")
    try:
        print("Getting all entries for duplicate analysis...")
        all_entries = get_all_records('data_for_api', 
                                    'smartphone_id,retailer_id,price,price_id,run_id', 
                                    run_id)
        
        analyze_duplicates(all_entries)
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
