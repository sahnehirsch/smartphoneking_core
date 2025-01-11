import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
from supabase import create_client, Client
import time

# Get the project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, 'hot_prices.log')),
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

class Config:
    # Batch processing
    BATCH_SIZE = 100
    
    # Hot price criteria
    MIN_TOP_PRICES = 5
    PRICE_THRESHOLD = 0.85  # 15% below average
    MIN_UNIQUE_RETAILERS = 2
    MIN_VERIFIED_RETAILERS = 1
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # seconds

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

@retry_on_error()
def batch_update_prices(updates):
    """Update prices in batches to reduce API calls"""
    if not updates:
        return
        
    failed_updates = []
    for i in range(0, len(updates), Config.BATCH_SIZE):
        batch = updates[i:i + Config.BATCH_SIZE]
        try:
            # Create individual updates for each price to maintain correct hotness scores
            for update in batch:
                result = supabase.table('prices').update({
                    'is_hot': True,
                    'hotness_score': update['hotness_score']
                }).eq('price_id', update['price_id']).execute()
                
                if hasattr(result, 'error') and result.error:
                    failed_updates.append(update)
                    logger.error(f"Error updating price {update['price_id']}: {result.error}")
        except Exception as e:
            failed_updates.extend(batch)
            logger.error(f"Exception updating batch: {str(e)}")
    
    if failed_updates:
        logger.error(f"Failed to update {len(failed_updates)} prices")
        raise Exception(f"Failed to update {len(failed_updates)} prices")

def update_hot_prices():
    """Update is_hot flag and hotness_score for prices based on defined criteria:
    1. Price must not be flagged as price_error
    2. Price must be at least 15% below top-5 average
    3. Top 5 prices must come from at least 2 different retailers
    4. At least one retailer in top 5 must be verified
    """
    try:
        # Reset hot flags and scores for today's prices
        today = datetime.utcnow().date().isoformat()
        reset_result = supabase.table('prices').update({
            'is_hot': False,
            'hotness_score': None,
            'date_recorded': datetime.utcnow().isoformat()
        }).gte('date_recorded', today).execute()
        
        if hasattr(reset_result, 'error') and reset_result.error:
            logger.error(f"Error resetting hot prices: {reset_result.error}")
            raise Exception(f"Failed to reset hot prices: {reset_result.error}")
            
        # Get the latest run_id
        latest_run = supabase.table('prices').select(
            'run_id'
        ).order('date_recorded', desc=True).limit(1).execute()
        
        if hasattr(latest_run, 'error') and latest_run.error:
            logger.error(f"Error getting latest run: {latest_run.error}")
            raise Exception(f"Failed to get latest run: {latest_run.error}")
            
        if not latest_run.data:
            logger.info("No prices found")
            return
            
        latest_run_id = latest_run.data[0]['run_id']
        if not latest_run_id:
            logger.info("No run_id found in latest prices")
            return
            
        logger.info(f"Processing prices from run_id: {latest_run_id}")
        
        # Get all prices from this run with their related data
        latest_prices = supabase.table('prices').select(
            'price_id,smartphone_id,retailer_id,price,date_recorded,price_error,smartphones(oem,model),retailers(relevance_status)'
        ).eq('run_id', latest_run_id).execute()
        
        if hasattr(latest_prices, 'error') and latest_prices.error:
            logger.error(f"Error getting latest prices: {latest_prices.error}")
            raise Exception(f"Failed to get latest prices: {latest_prices.error}")
            
        # Process prices and calculate hot prices
        smartphone_prices = {}
        for price in latest_prices.data:
            if price is None or 'price_error' not in price:
                logger.warning(f"Skipping invalid price record: {price}")
                continue
                
            if (not price['price_error'] and 
                price.get('retailers', {}).get('relevance_status') in ['VERIFIED', 'ACTIVE']):
                
                if price['smartphone_id'] not in smartphone_prices:
                    smartphone_prices[price['smartphone_id']] = []
                    
                smartphone_prices[price['smartphone_id']].append({
                    'price_id': price['price_id'],
                    'price': price['price'],
                    'retailer_id': price['retailer_id'],
                    'relevance_status': price['retailers']['relevance_status'],
                    'oem': price.get('smartphones', {}).get('oem'),
                    'model': price.get('smartphones', {}).get('model')
                })
        
        # Calculate hot prices for each smartphone
        price_updates = []
        hot_prices_info = []  # For logging
        
        for smartphone_id, prices in smartphone_prices.items():
            if not prices:  # Skip if no valid prices
                continue
                
            # Sort prices and get top 5
            top_5_prices = sorted(prices, key=lambda x: x['price'])[:Config.MIN_TOP_PRICES]
            
            if len(top_5_prices) < Config.MIN_TOP_PRICES:
                continue
                
            # Calculate metrics
            avg_top_5_price = sum(p['price'] for p in top_5_prices) / len(top_5_prices)
            unique_retailers = len(set(p['retailer_id'] for p in top_5_prices))
            verified_count = sum(1 for p in top_5_prices if p['relevance_status'] == 'VERIFIED')
            
            # Check each price against criteria
            for price in prices:
                if price['price'] <= 0:  # Skip invalid prices
                    logger.warning(f"Skipping non-positive price: {price}")
                    continue
                    
                if (price['price'] < avg_top_5_price * Config.PRICE_THRESHOLD and
                    unique_retailers >= Config.MIN_UNIQUE_RETAILERS and
                    verified_count >= Config.MIN_VERIFIED_RETAILERS):
                    
                    hotness_score = round((avg_top_5_price - price['price']) / avg_top_5_price * 100, 2)
                    
                    # Add to batch update
                    price_updates.append({
                        'price_id': price['price_id'],
                        'is_hot': True,
                        'hotness_score': hotness_score
                    })
                    
                    # Store info for logging
                    hot_prices_info.append({
                        'oem': price['oem'],
                        'model': price['model'],
                        'price': price['price'],
                        'hotness_score': hotness_score
                    })
        
        # Perform batch updates
        if price_updates:
            batch_update_prices(price_updates)
        
        # Log results
        logger.info(f"Found {len(hot_prices_info)} hot prices in latest run")
        
        for price in sorted(hot_prices_info, key=lambda x: x['hotness_score'], reverse=True):
            logger.info(
                f"Hot price: {price['oem']} {price['model']} "
                f"at {price['price']} "
                f"(hotness score: {price['hotness_score']}%)"
            )
                
    except Exception as e:
        logger.error(f"Error updating hot prices: {str(e)}")
        raise

if __name__ == "__main__":
    update_hot_prices()
