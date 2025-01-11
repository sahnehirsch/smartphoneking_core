# Smartphone Price Tracker Core

Core scripts for fetching and processing smartphone prices.

## Scripts

- `price_fetcher.py`: Fetches smartphone prices from various retailers
- `process_price_data.py`: Processes and validates the fetched price data
- `update_hot_prices.py`: Updates hot price flags based on price analysis
- `flag_price_errors.py`: Identifies and flags potential pricing errors
- `update_api_data.py`: Updates API data for frontend consumption

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

## Running the Scripts

The scripts should be run in the following order:

1. `price_fetcher.py` - Fetches new prices
2. `process_price_data.py` - Processes the fetched data
3. `flag_price_errors.py` - Flags any price errors
4. `update_hot_prices.py` - Updates hot price flags
5. `update_api_data.py` - Updates API data
