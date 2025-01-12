# Smartphone Price Tracker Core

Core scripts for fetching and processing smartphone prices.

## Scripts

- `price_fetcher.py`: Fetches smartphone prices from various retailers using Google Shopping API
- `process_price_data.py`: Processes and validates the fetched price data
- `flag_price_errors.py`: Identifies and flags potential pricing errors
- `update_hot_prices.py`: Updates hot price flags based on price analysis
- `update_api_data.py`: Updates API data for frontend consumption

## Automation Chain

The price tracking system follows an automated trigger chain:

1. **Entry Point: `price_fetcher.py`**
   - Fetches raw price data from Google Shopping API
   - Stores responses in `api_responses` and `api_response_data` tables
   - Automatically triggers `process_price_data.py` upon completion

2. **Data Processing: `process_price_data.py`**
   - Processes raw price data from `api_response_data` table
   - Inserts processed prices into the `prices` table
   - Triggers three sequential operations:
     1. Calls `flag_price_errors.py`
     2. Then calls `update_hot_prices.py`
     3. Finally calls `update_data_for_api.py`

3. **Price Validation: `flag_price_errors.py`**
   - Identifies and flags suspicious price entries
   - Called automatically by `process_price_data.py`

4. **Hot Prices: `update_hot_prices.py`**
   - Updates hot/trending prices
   - Called automatically after price error flagging

5. **API Data: `update_api_data.py`**
   - Final step in the chain
   - Updates data for API consumption
   - Called automatically after hot prices update

The entire process is triggered by running just `price_fetcher.py`. Each step waits for the previous one to complete, and comprehensive error handling ensures failures are logged but don't break the chain.

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```

## Environment Configuration

The `.env` file requires the following configuration:

### API Keys
- `SERPAPI_API_KEY`: Your Google Shopping API key for price fetching

### Database Configuration
- `SUPABASE_URL`: URL of your Supabase instance
- `SUPABASE_KEY`: Service role key for Supabase authentication
- `DB_CONNECTION_STRING`: PostgreSQL connection string in format:
  ```
  postgresql://postgres:[YOUR-PASSWORD]@your_db_host:5432/postgres
  ```

### API Settings
- `UPDATE_INTERVAL_HOURS`: How often to update prices (default: 6)
- `MAX_RETRIES`: Maximum number of retries for failed API calls (default: 3)
- `TIMEOUT_SECONDS`: API call timeout duration (default: 30)

All these settings must be properly configured for the system to work. The `.env.example` file provides a template with the correct structure and default values where applicable.

## Running the System

Simply run the entry point script:

```bash
python src/price_fetcher.py
```

This will automatically trigger the entire processing chain. The system is idempotent and tracks the last processed response_id to avoid duplicate processing.
