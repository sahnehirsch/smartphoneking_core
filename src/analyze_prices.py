import os
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

def get_all_prices():
    """Get all prices with pagination"""
    all_data = []
    start = 0
    batch_size = 1000
    
    # First, get total count
    count_result = supabase.table('data_for_api').select(
        '*', count='exact'
    ).limit(1).execute()
    
    if hasattr(count_result, 'error') and count_result.error:
        print(f"Error getting count: {count_result.error}")
        return None
        
    total_count = count_result.count
    print(f"Total records to retrieve: {total_count:,}")
    
    # Now get all data in batches
    while start < total_count:
        try:
            result = supabase.table('data_for_api').select(
                'smartphone_id,oem,model,price,is_hot,hotness_score'
            ).range(start, start + batch_size - 1).execute()
            
            if hasattr(result, 'error') and result.error:
                print(f"Error getting data batch: {result.error}")
                return None
                
            batch_data = result.data
            all_data.extend(batch_data)
            print(f"Retrieved {len(batch_data):,} records (total so far: {len(all_data):,} of {total_count:,})")
            
            if not batch_data:
                break
                
            start += batch_size
            
        except Exception as e:
            print(f"Error retrieving batch starting at {start}: {str(e)}")
            return None
    
    return all_data

def analyze_prices():
    """Analyze price distributions and hot prices in data_for_api table"""
    print("\n=== Price Analysis Report ===")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Get all prices
    data = get_all_prices()
    if not data:
        print("Error retrieving data")
        return
        
    # Convert to pandas DataFrame
    df = pd.DataFrame(data)
    
    # Basic statistics
    print("=== Basic Statistics ===")
    print(f"Total number of prices: {len(df):,}")
    print(f"Number of unique phones: {df['smartphone_id'].nunique():,}")
    print(f"Number of hot prices: {df['is_hot'].sum():,}")
    print(f"Percentage of hot prices: {(df['is_hot'].sum() / len(df)) * 100:.2f}%")
    
    # Price distribution
    print("\n=== Price Distribution (MXN) ===")
    print(f"Minimum price: {df['price'].min():,.2f}")
    print(f"Maximum price: {df['price'].max():,.2f}")
    print(f"Mean price: {df['price'].mean():,.2f}")
    print(f"Median price: {df['price'].median():,.2f}")
    
    # Percentiles
    percentiles = [25, 50, 75, 90, 95]
    print("\n=== Price Percentiles ===")
    for p in percentiles:
        print(f"{p}th percentile: {df['price'].quantile(p/100):,.2f}")
    
    # Prices per phone
    prices_per_phone = df.groupby(['oem', 'model']).agg({
        'price': ['count', 'min', 'max', 'mean'],
        'is_hot': 'sum'
    }).round(2)
    
    print("\n=== Top 10 Phones by Number of Prices ===")
    prices_per_phone_sorted = prices_per_phone.sort_values(('price', 'count'), ascending=False)
    for idx, row in prices_per_phone_sorted.head(10).iterrows():
        print(f"{idx[0]} {idx[1]}:")
        print(f"  - Number of prices: {row[('price', 'count')]:,.0f}")
        print(f"  - Price range: {row[('price', 'min')]:,.2f} - {row[('price', 'max')]:,.2f}")
        print(f"  - Average price: {row[('price', 'mean')]:,.2f}")
        print(f"  - Hot prices: {row[('is_hot', 'sum')]:,.0f}")
    
    # Hot prices analysis
    print("\n=== Hot Prices Analysis ===")
    hot_df = df[df['is_hot']].copy()  # Create a copy to avoid SettingWithCopyWarning
    if len(hot_df) > 0:
        print(f"Average hotness score: {hot_df['hotness_score'].mean():.2f}")
        print(f"Average price of hot items: {hot_df['price'].mean():,.2f}")
        print(f"Average price of non-hot items: {df[~df['is_hot']]['price'].mean():,.2f}")
        
        print("\n=== Top 5 OEMs by Hot Prices ===")
        hot_by_oem = hot_df.groupby('oem').size().sort_values(ascending=False).head()
        for oem, count in hot_by_oem.items():
            print(f"{oem}: {count:,} hot prices")
            
        # Additional hot price statistics
        print("\n=== Hot Prices Distribution ===")
        print("Number of hot prices by price range:")
        price_ranges = [0, 5000, 10000, 15000, 20000, float('inf')]
        labels = ['0-5k', '5k-10k', '10k-15k', '15k-20k', '20k+']
        hot_df.loc[:, 'price_range'] = pd.cut(hot_df['price'], bins=price_ranges, labels=labels)
        print(hot_df['price_range'].value_counts().sort_index())

if __name__ == '__main__':
    analyze_prices()
