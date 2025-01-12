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
    page = 0
    while True:
        try:
            query = supabase.table(table).select(select_query).eq('run_id', run_id)
            
            # Add any extra conditions
            if extra_conditions:
                for key, value in extra_conditions.items():
                    query = query.eq(key, value)
            
            # Add pagination
            result = query.range(
                page * page_size,
                (page + 1) * page_size - 1
            ).execute()
            
            if not result.data:
                break
                
            all_records.extend(result.data)
            
            if len(result.data) < page_size:
                break
                
            page += 1
            print(f"Retrieved {len(all_records):,} records so far...")
            
        except Exception as e:
            print(f"Error retrieving records: {e}", file=sys.stderr)
            break
    
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
        
        # Process results to find duplicates
        entry_counts = defaultdict(list)  # Changed to list to store price_ids
        for entry in all_entries:
            key = f"{entry['smartphone_id']}-{entry['retailer_id']}-{entry['price']}"
            entry_counts[key].append(entry['price_id'])
        
        duplicates = {k: v for k, v in entry_counts.items() if len(v) > 1}
        print(f"\nFound {len(duplicates)} keys with duplicates")
        
        if duplicates:
            print("\nTop 5 duplicated entries:")
            for key, price_ids in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
                print(f"\nKey {key}:")
                print(f"  {len(price_ids)} occurrences")
                print(f"  Price IDs: {sorted(price_ids)}")
                
                # Analyze sequential patterns
                patterns = analyze_sequential_patterns(price_ids)
                if patterns['total_sequences'] > 0:
                    print(f"  Sequential patterns found:")
                    print(f"    Number of sequences: {patterns['total_sequences']}")
                    print(f"    Longest sequence: {patterns['longest_sequence']} IDs")
                    print(f"    Sample sequences: {patterns['sequences']}")
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
