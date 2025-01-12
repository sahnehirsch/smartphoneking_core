import os
from dotenv import load_dotenv
import logging
from supabase import create_client, Client
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
import time
import re
from urllib.parse import urlparse

# Get the project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ensure log directory exists
os.makedirs(PROJECT_ROOT, exist_ok=True)

# Configure logging with more verbose settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, 'update_api_data.log'), mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add a startup message to verify logging
logger.info("=" * 80)
logger.info("Starting update_api_data script")
logger.info(f"Log file location: {os.path.join(PROJECT_ROOT, 'update_api_data.log')}")
logger.info("=" * 80)

# Load environment variables once at module level
load_dotenv()

# Initialize Supabase client once at module level
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

class Config:
    """Configuration settings for the script"""
    # Batch processing
    BATCH_SIZE = 1000  # For batching inserts
    PAGE_SIZE = 5000  # Increased from 1000 to reduce round trips
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # seconds
    
    # Validation
    MIN_PRICE = 0
    MAX_PRICE = 100000
    URL_PATTERNS = [
        r'^https?://[^\s/$.?#].[^\s]*$'  # Basic URL validation
    ]

def retry_on_error(max_retries: int = Config.MAX_RETRIES, delay: int = Config.RETRY_DELAY):
    """Decorator to retry operations on failure"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay} seconds...")
                        time.sleep(delay)
            logger.error(f"All {max_retries} attempts failed. Last error: {str(last_error)}")
            raise last_error
        return wrapper
    return decorator

def validate_price(price: float) -> bool:
    """Validate price is within acceptable range"""
    try:
        price_float = float(price)
        return Config.MIN_PRICE <= price_float <= Config.MAX_PRICE
    except (TypeError, ValueError):
        return False

def validate_url(url: str) -> bool:
    """Validate URL format"""
    if not url:
        return True  # Allow empty URLs
    
    # Basic URL validation
    try:
        result = urlparse(url)
        if not all([result.scheme, result.netloc]):
            return False
        return any(re.match(pattern, url) for pattern in Config.URL_PATTERNS)
    except Exception:
        return False

def clean_product_url(url: str) -> str:
    """Clean product URL by removing query parameters"""
    if not url:
        return url
    return url.split('?')[0] if '?' in url else url

@retry_on_error()
def get_latest_run_id() -> Optional[str]:
    """Get the latest run_id from prices table"""
    try:
        # Get the run_id with the latest date_recorded timestamp
        result = supabase.table('prices').select(
            'run_id,date_recorded'
        ).order('date_recorded', desc=True).limit(1).execute()
        
        if hasattr(result, 'error') and result.error:
            logger.error(f"Error getting latest run: {result.error}")
            return None
            
        if not result.data:
            logger.error("No runs found in prices table")
            return None
            
        run_id = result.data[0]['run_id']
        logger.info(f"Using latest run_id: {run_id} (recorded at: {result.data[0]['date_recorded']})")
        return run_id
    except Exception as e:
        logger.error(f"Error getting latest run_id: {e}")
        return None

@retry_on_error()
def get_valid_prices(run_id: str, page: int) -> Tuple[List[Dict], bool]:
    """Get a page of valid prices, ordered by smartphone_id, retailer_id, price to ensure consistent selection"""
    try:
        offset = page * Config.PAGE_SIZE
        logger.debug(f"Fetching prices with offset {offset}, run_id {run_id}")
        
        # Use a single query with all necessary filters
        result = (supabase.table('prices')
                 .select('*')
                 .eq('run_id', run_id)
                 .eq('price_error', False)
                 .not_('price', 'is', 'null')  # Changed back to not_ as it's more efficient
                 .order('smartphone_id')
                 .order('retailer_id')
                 .order('price')
                 .limit(Config.PAGE_SIZE)
                 .offset(offset)
                 .execute())
        
        if not hasattr(result, 'data'):
            logger.error("No data returned from prices query")
            return [], False
            
        # Check if there are more pages by requesting one more record
        next_page = (supabase.table('prices')
                    .select('price_id')
                    .eq('run_id', run_id)
                    .eq('price_error', False)
                    .not_('price', 'is', 'null')
                    .order('smartphone_id')
                    .order('retailer_id')
                    .order('price')
                    .limit(1)
                    .offset(offset + Config.PAGE_SIZE)
                    .execute())
        
        has_more = bool(next_page.data) if hasattr(next_page, 'data') else False
        logger.info(f"Retrieved {len(result.data)} records for page {page} (has more: {has_more})")
        
        return result.data, has_more
    except Exception as e:
        logger.error(f"Error getting valid prices: {e}")
        return [], False

@retry_on_error()
def get_smartphones(smartphone_ids: List[int]) -> Optional[Dict]:
    """Get smartphones by IDs"""
    try:
        result = supabase.table('smartphones').select(
            'smartphone_id,oem,model,color_variant,ram_variant,rom_variant,variant_rank,os'
        ).in_('smartphone_id', smartphone_ids).execute()
        
        if hasattr(result, 'error') and result.error:
            logger.error(f"Error getting smartphones: {result.error}")
            return None
            
        return {s['smartphone_id']: s for s in result.data}
    except Exception as e:
        logger.error(f"Error getting smartphones: {e}")
        return None

@retry_on_error()
def get_retailers(retailer_ids: List[int]) -> Optional[Dict]:
    """Get retailers by IDs"""
    try:
        result = supabase.table('retailers').select(
            'retailer_id,retailer_name'
        ).in_('retailer_id', retailer_ids).execute()
        
        if hasattr(result, 'error') and result.error:
            logger.error(f"Error getting retailers: {result.error}")
            return None
            
        return {r['retailer_id']: r for r in result.data}
    except Exception as e:
        logger.error(f"Error getting retailers: {e}")
        return None

def insert_data_batch(batch: List[Dict]) -> bool:
    """Insert a batch of data into data_for_api table"""
    try:
        # First, delete any existing records with these price_ids to avoid conflicts
        price_ids = [item['price_id'] for item in batch]
        if price_ids:
            try:
                delete_result = supabase.table('data_for_api').delete().in_('price_id', price_ids).execute()
                logger.debug(f"Deleted {len(price_ids)} existing records")
            except Exception as e:
                logger.warning(f"Error deleting existing records: {e}")
        
        # Then insert new records
        try:
            result = supabase.table('data_for_api').insert(batch).execute()
            if hasattr(result, 'error') and result.error:
                if 'duplicate key value violates unique constraint' in str(result.error):
                    logger.warning(f"Duplicate key violation for price_ids: {price_ids}")
                    # Try one by one to identify problematic records
                    success_count = 0
                    for item in batch:
                        try:
                            single_result = supabase.table('data_for_api').insert([item]).execute()
                            if not (hasattr(single_result, 'error') and single_result.error):
                                success_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to insert price_id {item['price_id']}: {e}")
                    return success_count > 0
                else:
                    logger.error(f"Error inserting batch: {result.error}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error in insert_data_batch: {e}")
        return False

def safe_convert_hotness_score(score) -> int:
    """Safely convert hotness score to integer"""
    try:
        return int(float(score)) if score is not None else 0
    except (TypeError, ValueError):
        return 0

def get_existing_product_keys(run_id: str) -> Set[str]:
    """Get set of existing product keys to avoid duplicates"""
    try:
        result = supabase.table('data_for_api').select('smartphone_id,retailer_id,price').eq('run_id', run_id).execute()
        if hasattr(result, 'data'):
            return {f"{item['smartphone_id']}-{item['retailer_id']}-{item['price']}" for item in result.data}
        return set()
    except Exception as e:
        logger.error(f"Error getting existing product keys: {e}")
        return set()

def process_price_batch(prices: List[Dict], run_id: str, processed_price_ids: Set[str]) -> Tuple[List[Dict], int]:
    """Process a batch of prices and return prepared data and skip count"""
    data_for_api = []
    total_skipped = 0
    
    # Get all price_ids for batch verification
    price_ids = [price['price_id'] for price in prices]
    
    # Verify all prices in batch with a single query
    verify_result = (supabase.table('prices')
                    .select('price_id,price_error,price')
                    .in_('price_id', price_ids)
                    .execute())
    
    if not verify_result.data:
        logger.warning(f"Could not verify {len(price_ids)} prices, skipping batch")
        return [], len(price_ids)
    
    # Create a lookup dictionary for verified prices
    verified_prices = {p['price_id']: p for p in verify_result.data}
    
    for price in prices:
        price_id = price['price_id']
        
        # Skip if already processed
        if price_id in processed_price_ids:
            logger.debug(f"Skipping already processed price_id: {price_id}")
            total_skipped += 1
            continue
            
        # Check verification result
        verified_price = verified_prices.get(price_id)
        if not verified_price or verified_price['price_error'] or verified_price['price'] is None:
            logger.warning(f"Price {price_id} failed verification")
            total_skipped += 1
            continue
            
        # Process valid price
        try:
            data_for_api.append({
                'price_id': price_id,
                'run_id': run_id,
                'smartphone_id': price['smartphone_id'],
                'retailer_id': price['retailer_id'],
                'price': price['price'],
                'url': price['url'],
                'date_recorded': price['date_recorded']
            })
            processed_price_ids.add(price_id)
        except Exception as e:
            logger.error(f"Error processing price {price_id}: {e}")
            total_skipped += 1
            
    return data_for_api, total_skipped

def update_data_for_api() -> bool:
    """Update the data_for_api table with the latest price data"""
    start_time = datetime.utcnow()
    logger.info("Starting data_for_api update...")
    
    try:
        # Get the latest run_id
        latest_run = supabase.table('prices').select('run_id,date_recorded').order('date_recorded', desc=True).limit(1).execute()
        if not latest_run.data:
            logger.error("No price data found")
            return
        
        run_id = latest_run.data[0]['run_id']
        date_recorded = latest_run.data[0]['date_recorded']
        logger.info(f"Using latest run_id: {run_id} (recorded at: {date_recorded})")
        
        # Delete old records
        logger.info("Deleting old records from previous runs...")
        delete_result = supabase.table('data_for_api').delete().neq('run_id', run_id).execute()
        logger.info(f"Delete result: {delete_result}")
        
        # Get total count for progress reporting
        count_result = supabase.table('prices').select('count', count='exact').eq('run_id', run_id).eq('price_error', False).not_('price', 'is', 'null').execute()
        total_count = count_result.count if hasattr(count_result, 'count') else 0
        logger.info(f"Total valid records to process: {total_count}")
        
        # Process data in pages
        page = 0
        total_processed = 0
        total_skipped = 0
        processed_price_ids = set()
        current_batch = []
        
        while True:
            # Get a page of prices
            prices, has_more = get_valid_prices(run_id, page)
            if not prices:
                break
                
            # Process prices in batches
            current_batch.extend(prices)
            while len(current_batch) >= Config.BATCH_SIZE:
                batch = current_batch[:Config.BATCH_SIZE]
                current_batch = current_batch[Config.BATCH_SIZE:]
                
                data_for_api, skipped = process_price_batch(batch, run_id, processed_price_ids)
                total_skipped += skipped
                
                if data_for_api:
                    try:
                        insert_result = supabase.table('data_for_api').insert(data_for_api).execute()
                        total_processed += len(data_for_api)
                    except Exception as e:
                        logger.error(f"Error inserting batch: {e}")
                        total_skipped += len(data_for_api)
                
                logger.info(f"Progress: {total_processed}/{total_count} records processed ({total_skipped} skipped)")
            
            if not has_more:
                break
            page += 1
        
        # Process remaining batch
        if current_batch:
            data_for_api, skipped = process_price_batch(current_batch, run_id, processed_price_ids)
            total_skipped += skipped
            
            if data_for_api:
                try:
                    insert_result = supabase.table('data_for_api').insert(data_for_api).execute()
                    total_processed += len(data_for_api)
                except Exception as e:
                    logger.error(f"Error inserting final batch: {e}")
                    total_skipped += len(data_for_api)
        
        logger.info(f"Finished processing {total_processed} records in {time.time() - start_time.time():.1f} seconds")
        logger.info(f"Success: {total_processed}, Skipped: {total_skipped}")
        return True
        
    except Exception as e:
        logger.error(f"Error in update_data_for_api: {e}")
        return False

if __name__ == '__main__':
    update_data_for_api()
