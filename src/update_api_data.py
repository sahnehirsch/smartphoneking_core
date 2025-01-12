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
    # Batch processing
    BATCH_SIZE = 100
    PAGE_SIZE = 1000
    
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
        result = (supabase.table('prices')
                 .select('*')
                 .eq('run_id', run_id)
                 .eq('price_error', False)
                 .filter('price', 'not.is', 'null')
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
                    .filter('price', 'not.is', 'null')
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

def update_data_for_api() -> bool:
    """Update the data_for_api table with the latest prices."""
    start_time = datetime.utcnow()
    logger.info("Starting data_for_api update...")
    
    try:
        # Get the latest run_id
        run_id = get_latest_run_id()
        if not run_id:
            logger.error("Could not get latest run_id")
            return False
        
        logger.info(f"Using data from run {run_id}")
        
        # Process data in pages
        page = 0
        total_processed = 0
        total_skipped = 0
        total_success = 0
        
        # First, delete old records from previous runs
        logger.info("Deleting old records from previous runs...")
        try:
            delete_result = supabase.table('data_for_api').delete().neq('run_id', run_id).execute()
            logger.info("Successfully deleted old records")
        except Exception as e:
            logger.error(f"Error deleting old records: {e}")
            return False
        
        # Get total count for progress reporting
        count_result = supabase.table('prices').select('*', count='exact').eq('run_id', run_id).eq('price_error', False).execute()
        total_count = count_result.count if hasattr(count_result, 'count') else 0
        logger.info(f"Total records to process: {total_count:,}")
        
        # Store processed price_ids and product keys to avoid duplicates
        processed_price_ids = set()
        processed_product_keys = set()
        
        while True:
            # Get page of prices with proper ordering
            prices, has_more = get_valid_prices(run_id, page)
            if not prices:
                if page == 0:
                    logger.error("Failed to get prices for first page")
                    return False
                break
                
            logger.info(f"Processing page {page} ({len(prices)} records)")
            
            # Get relevant smartphones and retailers for this batch
            smartphone_ids = list(set(p['smartphone_id'] for p in prices))
            retailer_ids = list(set(p['retailer_id'] for p in prices))
            
            smartphones = get_smartphones(smartphone_ids)
            retailers = get_retailers(retailer_ids)
            
            if not smartphones:
                logger.error("Could not get smartphones data")
                return False
            
            if not retailers:
                logger.error("Could not get retailers data")
                return False
            
            # Process and prepare data
            data_for_api = []
            
            for price in prices:
                # Recheck price_error status before processing
                price_check = supabase.table('prices').select('price_error').eq('price_id', price['price_id']).execute()
                if price_check.data and price_check.data[0]['price_error']:
                    logger.debug(f"Skipping price_id {price['price_id']} as it was flagged as error after initial retrieval")
                    total_skipped += 1
                    continue
                
                # Skip if already processed
                if price['price_id'] in processed_price_ids:
                    logger.debug(f"Skipping already processed price_id: {price['price_id']}")
                    total_skipped += 1
                    continue
                    
                # Skip invalid prices
                if not validate_price(price['price']):
                    logger.warning(f"Skipping invalid price: {price['price']}")
                    total_skipped += 1
                    continue
                    
                # Skip invalid URLs
                if not validate_url(price.get('product_url')):
                    logger.warning(f"Skipping invalid URL: {price.get('product_url')}")
                    total_skipped += 1
                    continue
                
                smartphone = smartphones.get(price['smartphone_id'])
                retailer = retailers.get(price['retailer_id'])
                
                if not smartphone:
                    logger.warning(f"Skipping price due to missing smartphone data: {price['smartphone_id']}")
                    total_skipped += 1
                    continue
                
                if not retailer:
                    logger.warning(f"Skipping price due to missing retailer data: {price['retailer_id']}")
                    total_skipped += 1
                    continue
                
                # Check for duplicate product key
                product_key = f"{price['smartphone_id']}-{price['retailer_id']}-{price['price']}"
                if product_key in processed_product_keys:
                    logger.debug(f"Skipping duplicate product: {product_key}")
                    total_skipped += 1
                    continue
                
                data_for_api.append({
                    'price_id': price['price_id'],
                    'smartphone_id': price['smartphone_id'],
                    'retailer_id': price['retailer_id'],
                    'retailer_name': retailer['retailer_name'],
                    'price': price['price'],
                    'product_url': clean_product_url(price.get('product_url')),
                    'is_hot': price.get('is_hot', False),
                    'hotness_score': safe_convert_hotness_score(price.get('hotness_score')),
                    'oem': smartphone['oem'],
                    'model': smartphone['model'],
                    'color_variant': smartphone.get('color_variant'),
                    'ram_variant': smartphone.get('ram_variant'),
                    'rom_variant': smartphone.get('rom_variant'),
                    'variant_rank': smartphone.get('variant_rank'),
                    'os': smartphone.get('os'),
                    'run_id': price['run_id']
                })
                processed_price_ids.add(price['price_id'])
                processed_product_keys.add(product_key)
            
            # Insert in batches
            if data_for_api:
                for i in range(0, len(data_for_api), Config.BATCH_SIZE):
                    batch = data_for_api[i:i + Config.BATCH_SIZE]
                    if insert_data_batch(batch):
                        total_success += len(batch)
                    total_processed += len(batch)
                    
                logger.info(f"Progress: {total_processed:,}/{total_count:,} records processed ({total_skipped:,} skipped)")
            
            if not has_more:
                break
            page += 1
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Finished processing {total_processed:,} records in {duration:.1f} seconds")
        logger.info(f"Success: {total_success:,}, Skipped: {total_skipped:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in update_data_for_api: {e}")
        return False

if __name__ == '__main__':
    update_data_for_api()
