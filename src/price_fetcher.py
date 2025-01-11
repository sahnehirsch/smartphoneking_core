import os
from datetime import datetime
from dotenv import load_dotenv
from serpapi import GoogleSearch
import json
import time
import logging
from typing import Dict, Any, Optional, Tuple, List
from supabase import create_client, Client
import httpx
import asyncio
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Add file handler separately with proper encoding
file_handler = logging.FileHandler('price_fetcher.log', mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
file_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class PriceFetcher:
    def __init__(self, run_id: str = None):
        self.api_key = os.getenv('SERPAPI_API_KEY')
        if not self.api_key:
            raise ValueError("SERPAPI_API_KEY not found in environment variables")
        
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.timeout = int(os.getenv('TIMEOUT_SECONDS', '30'))
        self.batch_size = 50  # Number of items to batch insert
        self.run_id = run_id or str(uuid.uuid4())  # Use provided run_id or generate new one
        if not run_id:
            logger.info(f"Starting new price fetching run with ID: {self.run_id}")
        
    def fetch_prices(self, search_query: str) -> Dict[str, Any]:
        """
        Fetch prices from SerpAPI with retry mechanism
        Only retries on specific transient errors like network issues or rate limits.
        """
        logger.info(f"Fetching prices for query: {search_query}")
        params = {
            "engine": "google_shopping",
            "q": search_query,
            "location": "Mexico",
            "google_domain": "google.com.mx",
            "gl": "mx",
            "hl": "es",
            "api_key": self.api_key
        }
        
        # Define which exceptions should trigger a retry
        RETRYABLE_EXCEPTIONS = (
            httpx.TimeoutException,  # Network timeouts
            httpx.NetworkError,      # Network connectivity issues
            httpx.TransportError,    # Low-level network issues
            Exception               # Temporarily include general exceptions for SerpAPI errors
        )
        
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    logger.warning(f"Retry attempt {attempt}/{self.max_retries - 1} for query: {search_query}")
                search = GoogleSearch(params)
                results = search.get_dict()
                
                # Validate the response
                if not results or not isinstance(results, dict):
                    logger.error(f"Invalid response format from API: {results}")
                    raise ValueError("Invalid API response format")
                    
                # Check if we got an error response
                if 'error' in results:
                    error_msg = results.get('error', '')
                    if any(retryable in error_msg.lower() for retryable in ['rate limit', 'timeout', 'temporary']):
                        if attempt == self.max_retries - 1:
                            raise ValueError(f"API Error: {error_msg}")
                        logger.warning(f"Retryable API error: {error_msg}")
                        time.sleep(min(2 ** attempt, 30))  # Cap the delay at 30 seconds
                        continue
                    else:
                        # Non-retryable API error
                        raise ValueError(f"API Error: {error_msg}")
                
                # Check if we have shopping results
                if 'shopping_results' not in results:
                    logger.warning(f"No shopping results found for query: {search_query}")
                    return results  # Return anyway as this might be valid (no results found)
                    
                logger.info(f"Successfully got {len(results.get('shopping_results', []))} results for query: {search_query}")
                return results
                
            except RETRYABLE_EXCEPTIONS as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to fetch prices after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Retryable error occurred: {str(e)}")
                time.sleep(min(2 ** attempt, 30))  # Cap the delay at 30 seconds
                
            except Exception as e:
                # Non-retryable exception - fail fast
                logger.error(f"Non-retryable error occurred: {str(e)}")
                raise

    def store_api_response(self, search_query: str, raw_response: Dict[str, Any]) -> Optional[int]:
        """Store raw API response in database"""
        try:
            # Convert response to JSON string with custom encoder
            response_json = json.dumps(raw_response, cls=DateTimeEncoder)
            
            # Store in database
            data = supabase.from_('api_responses').insert([
                {
                    'run_id': self.run_id,
                    'search_query': search_query,
                    'response_data': json.loads(response_json)  # Convert back to dict after serialization
                }
            ]).execute()
            
            # Get the ID from the response
            if data and data.data:
                return data.data[0].get('response_id')  # Use response_id instead of id
            return None
            
        except Exception as e:
            logger.error(f"Error storing API response: {str(e)}")
            return None

    def process_shopping_results(self, response_id: int, smartphone_id: int, results: Dict[str, Any]):
        """
        Process and store structured data from API response with batch processing
        """
        try:
            # Get the smartphone condition first
            data = supabase.from_('smartphones').select('condition').eq('smartphone_id', smartphone_id).execute()
            phone_condition = data.data[0]['condition']
            
            # Process each shopping result
            valid_results = 0
            total_results = len(results.get('shopping_results', []))
            batch_items = []
            
            for result in results.get('shopping_results', []):
                try:
                    # Skip if no price or price is 0
                    if 'extracted_price' not in result or result['extracted_price'] == 0:
                        continue
                        
                    # Skip if used phone when looking for new, or vice versa
                    title_lower = result.get('title', '').lower()
                    if (phone_condition == 'new' and ('reacondicionado' in title_lower or 
                        'refurbished' in title_lower or 
                        'usado' in title_lower or 
                        'used' in title_lower)):
                        logger.info(f"Skipping result for smartphone {smartphone_id}: Phone condition is new but item appears to be used")
                        continue

                    # Prepare item for batch insertion
                    batch_items.append({
                        'response_id': response_id,
                        'smartphone_id': smartphone_id,
                        'position': result.get('position'),
                        'product_id': result.get('product_id'),
                        'product_link': result.get('link'),
                        'extracted_price': result.get('extracted_price'),
                        'currency': result.get('currency', 'MXN'),
                        'source': result.get('source'),
                        'source_icon': result.get('source_icon'),
                        'store_rating': result.get('store_rating'),
                        'store_reviews': result.get('store_reviews'),
                        'product_rating': result.get('rating'),
                        'product_reviews': result.get('reviews'),
                        'second_hand_condition': result.get('second_hand_condition'),
                        'delivery_info': result.get('delivery')
                    })
                    valid_results += 1
                    
                    # Insert batch when it reaches batch_size
                    if len(batch_items) >= self.batch_size:
                        supabase.from_('api_response_data').insert(batch_items).execute()
                        batch_items = []
                    
                except Exception as e:
                    logger.error(f"Error processing individual result: {str(e)}")
                    continue
            
            # Insert any remaining items
            if batch_items:
                supabase.from_('api_response_data').insert(batch_items).execute()
            
            logger.info(f"Processed {valid_results} valid results out of {total_results} total for smartphone {smartphone_id}")
            
        except Exception as e:
            logger.error(f"Error processing shopping results: {str(e)}")
            raise

    def process_batch(self, smartphones_batch: List[Dict]) -> List[Tuple[int, Exception]]:
        """Process a batch of smartphones and return any errors"""
        errors = []
        for smartphone in smartphones_batch:
            try:
                logger.info(f"Processing smartphone ID: {smartphone['smartphone_id']}")
                results = self.fetch_prices(smartphone['search_query'])
                response_id = self.store_api_response(smartphone['search_query'], results)
                self.process_shopping_results(response_id, smartphone['smartphone_id'], results)
            except Exception as e:
                errors.append((smartphone['smartphone_id'], e))
        return errors

    def cleanup_old_responses(self):
        """
        Clean up old API responses, keeping only the most recent run.
        Returns the number of deleted runs.
        """
        try:
            # Get the most recent run_id (excluding current run)
            response = supabase.from_('api_responses')\
                .select('run_id')\
                .neq('run_id', self.run_id)\
                .order('created_at', desc=True)\
                .limit(1)\
                .execute()
            
            if not response.data:
                logger.info("No previous runs found to clean up")
                return 0
                
            latest_run_id = response.data[0]['run_id']
            
            # Delete all runs except the current one and the most recent one
            response = supabase.from_('api_responses')\
                .delete()\
                .neq('run_id', self.run_id)\
                .neq('run_id', latest_run_id)\
                .execute()
            
            deleted_count = len(response.data) if response.data else 0
            logger.info(f"Cleaned up {deleted_count} old API response runs")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old API responses: {str(e)}")
            # Don't raise the exception - this is a cleanup task that shouldn't block the main process
            return 0

def main():
    """Main function to update prices"""
    logger.info("Starting price update process")
    run_id = str(uuid.uuid4())
    logger.info(f"Starting new price fetching run with ID: {run_id}")
    
    try:
        fetcher = PriceFetcher(run_id=run_id)
        
        # Clean up old API responses before starting new run
        logger.info("Cleaning up old API responses...")
        fetcher.cleanup_old_responses()
        
        # Get active smartphones - only new condition
        data = supabase.from_('smartphones')\
            .select('smartphone_id, search_query, condition')\
            .eq('is_active', True)\
            .eq('condition', 'new')\
            .execute()
        smartphones = data.data
        logger.info(f"Found {len(smartphones)} active new smartphones to update")
        
        # Process smartphones in parallel batches
        batch_size = 5  # Number of smartphones to process in parallel
        all_errors = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(0, len(smartphones), batch_size):
                batch = smartphones[i:i + batch_size]
                errors = fetcher.process_batch(batch)
                all_errors.extend(errors)
                
                # Small delay between batches to avoid overwhelming the API
                if i + batch_size < len(smartphones):
                    time.sleep(0.5)
        
        # Log any errors that occurred
        if all_errors:
            logger.error(f"Encountered {len(all_errors)} errors during processing:")
            for smartphone_id, error in all_errors:
                logger.error(f"Error processing smartphone {smartphone_id}: {str(error)}")
        
        logger.info("Price fetch completed")
        
        # Import and run price data processing
        try:
            from process_price_data import process_price_data
            logger.info("Starting price processing")
            if process_price_data():
                logger.info("Successfully processed prices")
            else:
                logger.error("Price processing failed")
                raise Exception("Price processing returned False")
        except Exception as e:
            logger.error(f"Failed to process prices: {str(e)}")
            logger.error("Check process_price_data.log for more details")
            raise
            
        logger.info("Completed price update process")
        
    except Exception as e:
        logger.error(f"Error in price update process: {str(e)}")

if __name__ == "__main__":
    main()
