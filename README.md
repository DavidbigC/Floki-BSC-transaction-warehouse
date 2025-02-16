# Blockchain Transaction Tracker

This script is designed to track and store FLOKI token transactions from the BNB Chain (formerly BSC). It collects transaction data from multiple sources, processes them, and stores them in both a local PostgreSQL database and Dune Analytics.

## Features

- Collects transactions from multiple sources (Flipside Crypto and Dune Analytics)
- Processes transaction receipts using Alchemy API
- Stores transaction data and token metadata in a PostgreSQL database
- Syncs data with Dune Analytics
- Handles both BNB and WBNB denominated transactions
- Implements rate limiting and retry mechanisms
- Supports asynchronous processing for better performance

## Prerequisites

### API Keys Required
- Flipside API Key
- Alchemy API Key (BNB Chain)
- BSCScan API Key
- Dune Analytics API Key

### Database
- PostgreSQL database
- Database connection string format: `postgresql://username:password@host:port/database_name`

### Python Dependencies

- flipside-api-client
- web3
- requests
- pandas
- sqlalchemy


## Database Schema

### floki_transactions
- `tx_hash` (VARCHAR(66)) - Primary Key
- `transaction_type` (VARCHAR(4)) - BUY/SELL
- `user_address` (VARCHAR(42))
- `block_timestamp` (TIMESTAMP)
- `traded_token` (VARCHAR(42))
- `token_amount` (NUMERIC(78,18))
- `denominated_amount` (NUMERIC(78,18))
- `blockchain` (VARCHAR(20))

### token_contract
- `contract_address` (VARCHAR(42)) - Primary Key
- `token_name` (VARCHAR(255))
- `token_symbol` (VARCHAR(20))
- `decimals` (INTEGER)
- `blockchain` (VARCHAR(20))

## How It Works

1. **Initialization**
   - Sets up database schema if not exists
   - Establishes connections to various APIs

2. **Data Collection**
   - Fetches transactions from Flipside Crypto
   - Fetches transactions from Dune Analytics
   - Combines and deduplicates transactions

3. **Processing**
   - Identifies missing transactions not in local database
   - Processes transactions in batches asynchronously
   - Decodes token transfers and amounts
   - Collects token metadata for new tokens

4. **Storage**
   - Stores processed transactions in PostgreSQL
   - Updates token metadata
   - Syncs data to Dune Analytics


The script will:
1. Initialize the database schema
2. Collect transactions from all sources
3. Process any missing transactions
4. Update token metadata
5. Upload the final data to Dune Analytics

## Performance

- Processes transactions in batches of 30
- Implements rate limiting (30 requests per second)
- Uses asynchronous processing for better performance
- Includes retry mechanisms for failed requests

## Error Handling

- Implements retry logic for failed API requests
- Logs errors with detailed information
- Continues processing even if individual transactions fail
- Validates token metadata before storage

## Notes

- The script is specifically designed to track FLOKI token transactions
- Focuses on transactions involving the address: `0xea38f7588ff5c5f69d21f690942e8343c467e32a`
- Handles both native BNB and WBNB wrapped token transactions

