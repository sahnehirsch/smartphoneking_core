import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from collections import defaultdict

# Configuration
class Config:
    # Price thresholds
    MIN_PRICE_THRESHOLD = 1000
    MAX_PRICE_THRESHOLD = 60000
    DEFAULT_CURRENCY = 'MXN'
    
    # Processing settings
    BATCH_SIZE = 1000
    UPDATE_BATCH_SIZE = 50  # Reduced to avoid HTTP/2 connection limits
    
    # Current time
    CURRENT_TIME = datetime.utcnow()

# Get the project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, 'flag_price_errors.log')),
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

def get_latest_run_id():
    """Get the run_id from the most recent prices based on date_recorded"""
    result = supabase.table('prices').select('run_id').order('date_recorded', desc=True).limit(1).execute()
    if not result.data:
        raise ValueError("No prices found in database")
    return result.data[0]['run_id']

def get_total_prices(run_id: str):
    """Get total number of prices for a specific run"""
    result = supabase.table('prices').select('price_id', count='exact').eq('run_id', run_id).execute()
    return result.count

def get_price_batch(offset: int, limit: int, run_id: str):
    """Get a batch of prices from a specific run"""
    result = supabase.table('prices').select('*').eq('run_id', run_id).range(offset, offset + limit - 1).execute()
    return result.data

def batch_update_prices(updates_true, updates_false):
    """Update prices in batches to reduce API calls"""
    try:
        # First, get current records to preserve fields
        price_ids = [update['price_id'] for update in updates_true + updates_false]
        current_records = {}
        
        for i in range(0, len(price_ids), Config.BATCH_SIZE):
            batch_ids = price_ids[i:i + Config.BATCH_SIZE]
            result = supabase.table('prices').select('*').in_('price_id', batch_ids).execute()
            
            if result.data:
                for record in result.data:
                    current_records[record['price_id']] = record
        
        # Process true updates
        for i in range(0, len(updates_true), Config.UPDATE_BATCH_SIZE):
            batch = updates_true[i:i + Config.UPDATE_BATCH_SIZE]
            
            for update in batch:
                price_id = update['price_id']
                if price_id in current_records:
                    current = current_records[price_id]
                    update['date_recorded'] = datetime.utcnow().isoformat()
                    # Preserve other fields...
                    update['run_id'] = current.get('run_id')
                    update['product_url'] = current.get('product_url')
                    update['thumbnail'] = current.get('thumbnail')
                    update['is_hot'] = current.get('is_hot', False)
                    update['hotness_score'] = current.get('hotness_score')
            
            result = supabase.table('prices').upsert(batch).execute()
            if hasattr(result, 'error') and result.error:
                logger.error(f"Error updating batch: {result.error}")
                raise Exception(f"Failed to update batch: {result.error}")
        
        # Process false updates similarly
        for i in range(0, len(updates_false), Config.UPDATE_BATCH_SIZE):
            batch = updates_false[i:i + Config.UPDATE_BATCH_SIZE]
            
            for update in batch:
                price_id = update['price_id']
                if price_id in current_records:
                    current = current_records[price_id]
                    update['date_recorded'] = datetime.utcnow().isoformat()
                    # Preserve other fields...
                    update['run_id'] = current.get('run_id')
                    update['product_url'] = current.get('product_url')
                    update['thumbnail'] = current.get('thumbnail')
                    update['is_hot'] = current.get('is_hot', False)
                    update['hotness_score'] = current.get('hotness_score')
            
            result = supabase.table('prices').upsert(batch).execute()
            if hasattr(result, 'error') and result.error:
                logger.error(f"Error updating batch: {result.error}")
                raise Exception(f"Failed to update batch: {result.error}")
                
    except Exception as e:
        logger.error(f"Error in batch_update_prices: {str(e)}")
        raise

def flag_price_errors():
    """
    Update price_error flags for all prices in the latest run.
    Process:
    1. First flag NULL values in critical fields (price, retailer_id, smartphone_id)
    2. Then flag extreme prices (>60k MXN and <1k MXN)
    3. Calculate reference average using 5 MEDIAN prices per smartphone
    4. Flag prices that are >50% below or 2x above the reference average
    5. Set price_error to false for all other prices from this run
    """
    try:
        # Get the latest run_id
        run_id = get_latest_run_id()
        logger.info(f"Processing latest run: {run_id}")
        
        # Get total number of prices for this run
        total_prices = get_total_prices(run_id)
        logger.info(f"Found {total_prices} total prices to analyze for run {run_id}")
        
        # Process in batches
        offset = 0
        total_null = 0
        total_extreme = 0
        total_deviation = 0
        
        # Store all smartphone prices for average calculation
        smartphone_prices = defaultdict(list)
        # Store ALL price IDs from this run to mark non-flagged ones as valid
        all_price_ids = set()
        flagged_price_ids = set()
        
        # First pass: collect data and flag extreme prices
        while offset < total_prices:
            # Get batch of prices
            prices = get_price_batch(offset, Config.BATCH_SIZE, run_id)
            logger.info(f"Processing batch of {len(prices)} prices (offset: {offset})")
            
            # Process batch
            null_count = 0
            extreme_count = 0
            for price in prices:
                all_price_ids.add(price['price_id'])
                
                # Check for NULL values in critical fields
                if price.get('price') is None:
                    null_count += 1
                    flagged_price_ids.add(price['price_id'])
                    price_updates = [{
                        'price_id': price['price_id'], 
                        'price_error': True, 
                        'error_reason': 'Price is null', 
                        'date_recorded': datetime.utcnow().isoformat()
                    }]
                    batch_update_prices(price_updates, [])
                    logger.debug(f"Flagged price {price['price_id']} due to NULL price")
                    continue
                elif price.get('retailer_id') is None or price.get('smartphone_id') is None:
                    null_count += 1
                    flagged_price_ids.add(price['price_id'])
                    price_updates = [{
                        'price_id': price['price_id'], 
                        'price_error': True, 
                        'error_reason': 'Missing critical data (NULL values)', 
                        'date_recorded': datetime.utcnow().isoformat()
                    }]
                    batch_update_prices(price_updates, [])
                    logger.debug(f"Flagged price {price['price_id']} due to NULL values")
                    continue
                
                # Check for extreme prices only if price is not NULL
                if price['currency'] == Config.DEFAULT_CURRENCY and price['price'] is not None:
                    if price['price'] < Config.MIN_PRICE_THRESHOLD or price['price'] > Config.MAX_PRICE_THRESHOLD:
                        extreme_count += 1
                        flagged_price_ids.add(price['price_id'])
                        price_updates = [{
                            'price_id': price['price_id'], 
                            'price_error': True, 
                            'error_reason': 'Extreme price', 
                            'date_recorded': datetime.utcnow().isoformat()
                        }]
                        batch_update_prices(price_updates, [])
                        logger.debug(f"Flagged price {price['price_id']} as extreme: {price['price']}")
                    else:
                        # Store ALL non-extreme prices that don't have NULL values
                        smartphone_prices[price['smartphone_id']].append((price['price_id'], price['price']))
            
            total_null += null_count
            total_extreme += extreme_count
            logger.info(f"Found {null_count} prices with NULL values and {extreme_count} extreme prices in current batch")
            
            # Move to next batch
            offset += Config.BATCH_SIZE
        
        logger.info(f"Processing deviations for {len(smartphone_prices)} smartphones")
        
        # Second pass: calculate reference averages and flag deviations
        for smartphone_id, all_prices in smartphone_prices.items():
            if len(all_prices) < 5:  # Need at least 5 prices to calculate median range
                continue
                
            # Sort ALL prices by value
            sorted_prices = sorted(all_prices, key=lambda x: x[1])
            total_prices = len(sorted_prices)
            
            # Find the middle position
            mid_pos = total_prices // 2
            
            # Take 2 prices before median, median, and 2 prices after median
            # This gives us 5 prices centered around the median
            start_idx = max(0, mid_pos - 2)
            end_idx = min(total_prices, start_idx + 5)
            # Adjust start if we hit the end boundary
            if end_idx - start_idx < 5 and start_idx > 0:
                start_idx = max(0, end_idx - 5)
            
            median_prices = sorted_prices[start_idx:end_idx]
            
            # Calculate reference average using MEDIAN prices
            reference_values = [p[1] for p in median_prices]
            reference_avg = sum(reference_values) / len(reference_values)
            min_allowed = reference_avg * 0.5  # 50% below reference
            max_allowed = reference_avg * 2    # 2x above reference
            
            logger.debug(f"Smartphone {smartphone_id}:")
            logger.debug(f"  Reference avg: {reference_avg:.2f} (from {len(median_prices)} median prices)")
            logger.debug(f"  Median range used: {[p[1] for p in median_prices]}")
            logger.debug(f"  Allowed range: {min_allowed:.2f} - {max_allowed:.2f}")
            
            # Check ALL prices against reference average
            updates_true = []
            for price_id, price_value in all_prices:
                if price_value < min_allowed or price_value > max_allowed:
                    reason = (
                        'Price too low (below 50% of median reference)' 
                        if price_value < min_allowed 
                        else 'Price too high (above 2x median reference)'
                    )
                    updates_true.append({'price_id': price_id, 'price_error': True, 'error_reason': reason, 'date_recorded': datetime.utcnow().isoformat()})
                    flagged_price_ids.add(price_id)
                    total_deviation += 1
                    logger.debug(f"  Flagged: {price_value:.2f} ({reason})")
            
            # Update flagged prices in batch
            if updates_true:
                batch_update_prices(updates_true, [])
        
        # Finally, mark all non-flagged prices from this run as valid
        valid_price_ids = all_price_ids - flagged_price_ids
        logger.info(f"Marking {len(valid_price_ids)} prices as valid")
        
        # Update valid prices in batches
        valid_prices_list = list(valid_price_ids)
        for i in range(0, len(valid_prices_list), Config.UPDATE_BATCH_SIZE):
            batch = valid_prices_list[i:i + Config.UPDATE_BATCH_SIZE]
            batch_update_prices([], [{'price_id': price_id, 'price_error': False, 'error_reason': None, 'date_recorded': datetime.utcnow().isoformat()} for price_id in batch])
        
        logger.info(f"Finished processing all prices")
        logger.info(f"Total prices processed: {len(all_price_ids)}")
        logger.info(f"Total prices with NULL values: {total_null}")
        logger.info(f"Total extreme prices flagged: {total_extreme}")
        logger.info(f"Total deviating prices flagged: {total_deviation}")
        logger.info(f"Total prices marked as valid: {len(valid_price_ids)}")
        
    except Exception as e:
        logger.error(f"Error updating price errors: {str(e)}")
        logger.exception("Full traceback:")
        raise

if __name__ == "__main__":
    flag_price_errors()
