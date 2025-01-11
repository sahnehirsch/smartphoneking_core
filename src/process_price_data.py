import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
import logging
from supabase import create_client, Client
import uuid
from flag_price_errors import flag_price_errors
from update_hot_prices import update_hot_prices
from update_api_data import update_data_for_api

# Configuration
class Config:
    # Processing settings
    HOURS_TO_PROCESS = 24
    MAX_RETRIES = 3
    RETRY_DELAY = 1
    
    # Batch processing
    BATCH_SIZE = 100
    PAGE_SIZE = 1000
    
    # Default currency
    DEFAULT_CURRENCY = 'MXN'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process_prices.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

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
                    if attempt == max_retries - 1:
                        logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}, retrying...")
                    time.sleep(delay)
            raise last_error
        return wrapper
    return decorator

@retry_on_error()
def get_retailers() -> Dict[str, int]:
    """Get all retailers from database"""
    result = supabase.table('retailers').select('retailer_id,retailer_name').execute()
    if hasattr(result, 'error') and result.error:
        logger.error(f"Error getting retailers: {result.error}")
        raise Exception(f"Failed to get retailers: {result.error}")
        
    return {
        r['retailer_name'].lower(): r['retailer_id'] 
        for r in result.data
    }

@retry_on_error()
def get_last_processed_response_id() -> int:
    """Get the last processed response_id from the prices table"""
    result = supabase.table('prices').select('response_id').order('response_id', desc=True).limit(1).execute()
    if hasattr(result, 'error') and result.error:
        logger.error(f"Error getting last processed response_id: {result.error}")
        raise Exception(f"Failed to get last processed response_id: {result.error}")
        
    return result.data[0]['response_id'] if result.data else 0

@retry_on_error()
def get_api_response_data(last_processed_id: int) -> List:
    """Get API response data since the last processed response_id"""
    result = supabase.table('api_response_data').select(
        '*,api_responses(search_query,created_at,run_id)'
    ).gt('response_id', last_processed_id).order('response_id').execute()
    
    if hasattr(result, 'error') and result.error:
        logger.error(f"Error getting API response data: {result.error}")
        raise Exception(f"Failed to get API response data: {result.error}")
        
    return result.data

@retry_on_error()
def create_new_retailer(retailer_name: str) -> int:
    """Create a new retailer and return its ID."""
    logging.info(f"Found new retailer: {retailer_name}")
    
    try:
        # Get the maximum retailer_id
        result = supabase.table('retailers').select('retailer_id').order('retailer_id', desc=True).limit(1).execute()
        max_retailer_id = result.data[0]['retailer_id'] if result.data else 0
        
        # Create new retailer with incremented ID
        data = {
            'retailer_id': max_retailer_id + 1,
            'retailer_name': retailer_name,
            'relevance_status': 'SUSPICIOUS',  # New retailers start as suspicious
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        result = supabase.table('retailers').insert(data).execute()
        return data['retailer_id']
    except Exception as e:
        if 'duplicate key value violates unique constraint' in str(e):
            # If we hit a duplicate key, try again with the next ID
            return create_new_retailer(retailer_name)
        raise

@retry_on_error()
def insert_price(response_id, retailer_id, smartphone_id, price, currency, url, run_id=None, thumbnail=None) -> None:
    """Insert a new price record into the prices table."""
    try:
        # Check if price exists for today
        today = datetime.utcnow().date().isoformat()
        existing = supabase.table('prices').select('price_id').eq(
            'smartphone_id', smartphone_id
        ).eq('retailer_id', retailer_id).gte(
            'date_recorded', today
        ).execute()
        
        # Prepare price data
        price_data = {
            'response_id': response_id,
            'retailer_id': retailer_id,
            'smartphone_id': smartphone_id,
            'price': price,
            'currency': currency or Config.DEFAULT_CURRENCY,
            'product_url': url[:255] if url else None,  # Truncate URL if too long
            'thumbnail': thumbnail,
            'run_id': run_id,
            'date_recorded': datetime.utcnow().isoformat()
        }
        
        if existing.data:
            # Update existing price
            price_id = existing.data[0]['price_id']
            result = supabase.table('prices').update(price_data).eq('price_id', price_id).execute()
        else:
            # Insert new price
            result = supabase.table('prices').insert(price_data).execute()
            
        if hasattr(result, 'error') and result.error:
            logger.error(f"Error inserting/updating price: {result.error}")
            raise Exception(f"Failed to insert/update price: {result.error}")
            
    except Exception as e:
        logger.error(f"Error inserting price: {str(e)}")
        raise

def determine_phone_condition(second_hand_condition: str, snippet: str) -> str:
    """
    Determine if a phone is new or used based on the listing information
    """
    if not second_hand_condition and not snippet:
        return 'new'
        
    # Check second_hand_condition
    if second_hand_condition:
        second_hand = second_hand_condition.lower()
        if second_hand in ['usado', 'used', 'reacondicionado', 'refurbished']:
            return 'used'
            
    # Check snippet
    if snippet:
        snippet_lower = snippet.lower()
        if 'used' in snippet_lower or 'refurbished' in snippet_lower:
            return 'used'
            
    return 'new'

def process_batch(batch: List[dict], retailer_map: Dict[str, int]) -> None:
    """Process a batch of records."""
    for record in batch:
        source = record['source'].lower() if record['source'] else ''
        
        # Skip empty sources
        if not source:
            continue
        
        # Get or create retailer
        if source not in retailer_map:
            retailer_id = create_new_retailer(source)
            retailer_map[source] = retailer_id
        
        retailer_id = retailer_map[source]
        
        # Extract data
        response_id = record['response_id']
        smartphone_id = record['smartphone_id']
        price = record['extracted_price']
        currency = record['currency']
        url = record['product_link']
        run_id = record['api_responses']['run_id'] if record['api_responses'] else None
        thumbnail = record.get('thumbnail')
        
        # Insert price record
        insert_price(response_id, retailer_id, smartphone_id, price, currency, url, run_id, thumbnail)

def process_price_data():
    """
    Process price data from api_response_data table and insert into prices table.
    """
    start_time = time.time()
    try:
        # Get last processed response_id from api_response_data
        result = supabase.table('api_response_data').select('response_id').order('response_id', desc=True).limit(1).execute()
        max_response_id = result.data[0]['response_id'] if result.data else 0
        logger.info(f"Maximum response_id in api_response_data: {max_response_id}")

        # Get last processed response_id from prices
        result = supabase.table('prices').select('response_id').order('response_id', desc=True).limit(1).execute()
        last_response_id = result.data[0]['response_id'] if result.data else 0
        logger.info(f"Last processed response_id in prices: {last_response_id}")

        # Get retailer mapping
        result = supabase.table('retailers').select('retailer_id,retailer_name').execute()
        retailer_map = {r['retailer_name'].lower(): r['retailer_id'] for r in result.data}

        # Process records in pages
        current_response_id = last_response_id
        total_records = 0
        
        while True:
            # Get next page of records
            result = supabase.table('api_response_data').select(
                '*,api_responses(search_query,created_at,run_id)'
            ).gt('response_id', current_response_id).order('response_id').limit(Config.PAGE_SIZE).execute()

            if not result.data:
                break

            page_size = len(result.data)
            total_records += page_size
            logger.info(f"Processing {page_size} records (Total: {total_records}, Progress: {current_response_id}/{max_response_id})")

            # Process records in batches
            for i in range(0, page_size, Config.BATCH_SIZE):
                batch = result.data[i:i + Config.BATCH_SIZE]
                batch_num = (total_records - page_size + i) // Config.BATCH_SIZE + 1
                
                # Log batch info
                logger.info(f"Processing data for run ID: {batch[0]['api_responses']['run_id']}")
                logger.info(f"Processing up to response_id: {batch[-1]['response_id']}")
                
                # Process each record in batch
                process_batch(batch, retailer_map)
                
                logger.info(f"Processed batch {batch_num} ({min(i + Config.BATCH_SIZE, page_size)}/{page_size} records in current page)")

            # Update current_response_id for next page
            current_response_id = result.data[-1]['response_id']

            # Break if we've processed all records
            if current_response_id >= max_response_id:
                break

        logger.info(f"Finished processing {total_records} records from response_id {last_response_id} to {current_response_id}")

        # After all prices are inserted, trigger price error flagging
        try:
            logger.info("Starting price error flagging...")
            flag_price_errors()
            logger.info("Price error flagging completed successfully")
        except Exception as e:
            logger.error(f"Error during price error flagging: {str(e)}")

        # After price error flagging, update hot prices and API data
        try:
            logger.info("Starting hot prices update...")
            update_hot_prices()
            logger.info("Hot prices update completed successfully")
        except Exception as e:
            logger.error(f"Error updating hot prices: {str(e)}")

        try:
            logger.info("Starting API data update...")
            if update_data_for_api():
                logger.info("API data update completed successfully")
            else:
                logger.error("API data update failed")
        except Exception as e:
            logger.error(f"Error updating API data: {str(e)}")

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error processing price data after {elapsed:.2f} seconds: {str(e)}")
        raise

if __name__ == "__main__":
    process_price_data()
