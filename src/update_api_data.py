import os
from dotenv import load_dotenv
import logging
from supabase import create_client, Client
from typing import List, Dict, Optional, Tuple
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
        result = supabase.table('prices').select(
            'run_id'
        ).order('run_id', desc=True).limit(1).execute()
        
        if hasattr(result, 'error') and result.error:
            logger.error(f"Error getting latest run: {result.error}")
            return None
            
        return result.data[0]['run_id'] if result.data else None
    except Exception as e:
        logger.error(f"Error getting latest run_id: {e}")
        return None

@retry_on_error()
def get_valid_prices(run_id: str, page: int = 0) -> Tuple[Optional[List[Dict]], bool]:
    """Get valid prices for the given run_id with pagination"""
    try:
        # First, get total count
        count_result = supabase.table('prices').select(
            'price_id', count='exact'
        ).eq('run_id', run_id).eq('price_error', False).execute()
        
        if hasattr(count_result, 'error') and count_result.error:
            logger.error(f"Error getting total count: {count_result.error}")
            return None, False
            
        total_count = count_result.count if hasattr(count_result, 'count') else 0
        logger.info(f"Total valid prices for run {run_id}: {total_count}")
        
        # Get current page of data
        result = supabase.table('prices').select(
            'price_id,smartphone_id,retailer_id,price,product_url,is_hot,hotness_score,run_id'
        ).eq('run_id', run_id).eq('price_error', False).order('price_id').limit(
            Config.PAGE_SIZE
        ).offset(page * Config.PAGE_SIZE).execute()
        
        if hasattr(result, 'error') and result.error:
            logger.error(f"Error getting prices: {result.error}")
            return None, False
            
        current_page_size = len(result.data)
        logger.debug(f"Retrieved {current_page_size} records for page {page}")
        
        # Check if there are more pages
        has_more = (page + 1) * Config.PAGE_SIZE < total_count
        
        return result.data, has_more
    except Exception as e:
        logger.error(f"Error getting valid prices: {e}")
        return None, False

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
def insert_data_batch(batch: List[Dict]) -> bool:
    """Insert a batch of data into data_for_api table"""
    try:
        # First, delete any existing records with these price_ids
        price_ids = [item['price_id'] for item in batch]
        delete_result = supabase.table('data_for_api').delete().in_('price_id', price_ids).execute()
        
        # Then insert the new records
        result = supabase.table('data_for_api').insert(batch).execute()
        
        if hasattr(result, 'error') and result.error:
            logger.error(f"Error inserting batch: {result.error}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error inserting batch: {e}")
        return False

def safe_convert_hotness_score(score) -> int:
    """Safely convert hotness score to integer"""
    try:
        return int(float(score)) if score is not None else 0
    except (TypeError, ValueError):
        return 0

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
        delete_result = supabase.table('data_for_api').delete().neq('run_id', run_id).execute()
        
        # Get total count for progress reporting
        count_result = supabase.table('prices').select('*', count='exact').eq('run_id', run_id).eq('price_error', False).execute()
        total_count = count_result.count if hasattr(count_result, 'count') else 0
        logger.info(f"Total records to process: {total_count:,}")
        
        while True:
            # Get page of prices with proper ordering
            prices, has_more = get_valid_prices(run_id, page)
            if not prices:
                logger.error(f"Failed to get prices for page {page}")
                break
                
            # Get relevant smartphones for this batch
            smartphone_ids = list(set(p['smartphone_id'] for p in prices))
            smartphones = get_smartphones(smartphone_ids)
            if not smartphones:
                logger.error("Could not get smartphones data")
                return False
            
            # Process and prepare data
            data_for_api = []
            
            for price in prices:
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
                if not smartphone:
                    logger.warning(f"Skipping price due to missing smartphone data: {price['smartphone_id']}")
                    total_skipped += 1
                    continue
                
                data_for_api.append({
                    'price_id': price['price_id'],  # Include price_id in the data
                    'smartphone_id': price['smartphone_id'],
                    'retailer_id': price['retailer_id'],
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
            
            # Insert in batches
            for i in range(0, len(data_for_api), Config.BATCH_SIZE):
                batch = data_for_api[i:i + Config.BATCH_SIZE]
                if insert_data_batch(batch):
                    total_success += len(batch)
                else:
                    logger.error(f"Failed to insert batch of size {len(batch)}")
            
            total_processed += len(prices)
            logger.info(f"Processed {total_processed:,} records (Page {page})...")
            
            if not has_more:
                logger.info("No more pages to process")
                break
                
            page += 1
        
        if total_skipped > 0:
            logger.warning(f"Skipped {total_skipped} records due to validation or missing data")
        
        # Log completion
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            f"Successfully inserted {total_success:,}/{total_processed:,} records "
            f"in {execution_time:.2f} seconds"
        )
        
        # Show a sample of the data
        sample_query = supabase.table('data_for_api').select(
            'smartphone_id,oem,model,price,is_hot,hotness_score'
        ).limit(3).execute()
        
        if not hasattr(sample_query, 'error') or not sample_query.error:
            logger.info("Sample of inserted data:")
            for row in sample_query.data:
                logger.info(
                    f"{row['oem']} {row['model']}: "
                    f"${row['price']} "
                    f"({'HOT! ' if row['is_hot'] else ''}"
                    f"Score: {row['hotness_score']})"
                )
        
        return total_success > 0
        
    except Exception as e:
        logger.error(f"Error updating data_for_api: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == '__main__':
    update_data_for_api()
