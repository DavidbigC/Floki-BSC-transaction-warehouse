from flipside import Flipside
from web3 import Web3
from web3.middleware import geth_poa_middleware
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from tqdm import tqdm
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
from multiprocessing import cpu_count
from sqlalchemy.types import Numeric, String, Integer
import csv
from sqlalchemy.exc import IntegrityError
from dune_client.client import DuneClient
from decimal import Decimal, getcontext

# Initialize connections
FLIPSIDE_API_KEY = ""
ALCHEMY_URL = "https://bnb-mainnet.g.alchemy.com/v2/"
RATE_LIMIT = 30  # requests per second
BSCSCAN_API_KEY = ""  # Get from bscscan.com
DUNE_API_KEY = ""  # From your notebook

# Initialize connections
flipside = Flipside(FLIPSIDE_API_KEY, "https://api-v2.flipsidecrypto.xyz")
w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)
engine = create_engine('postgresql://postgres:__@localhost:__/__')

def initialize_database_schema():
    """Create tables if they don't exist with correct schema"""
    with engine.connect() as connection:
        # Create floki_transactions table
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS floki_transactions (
                tx_hash VARCHAR(66) PRIMARY KEY,
                transaction_type VARCHAR(4),
                user_address VARCHAR(42),
                block_timestamp TIMESTAMP,
                traded_token VARCHAR(42),
                token_amount NUMERIC(78,18),
                denominated_amount NUMERIC(78,18),
                blockchain VARCHAR(20)
            )
        """))
        
        # Create token_contract table with correct schema
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS token_contract (
                contract_address VARCHAR(42) PRIMARY KEY,
                token_name VARCHAR(255),
                token_symbol VARCHAR(20),
                decimals INTEGER,
                blockchain VARCHAR(20)
            )
        """))
        connection.commit()

def get_flipside_transactions():
    """Get transactions from Flipside API"""
    try:
    sql = """
    SELECT DISTINCT TX_HASH
    FROM (
        SELECT TX_HASH
        FROM bsc.core.ez_native_transfers
        WHERE FROM_ADDRESS = '0xea38f7588ff5c5f69d21f690942e8343c467e32a'
        OR TO_ADDRESS = '0xea38f7588ff5c5f69d21f690942e8343c467e32a'
        
        UNION
        
        SELECT TX_HASH
        FROM bsc.core.ez_token_transfers
        WHERE FROM_ADDRESS = '0xea38f7588ff5c5f69d21f690942e8343c467e32a'
        OR TO_ADDRESS = '0xea38f7588ff5c5f69d21f690942e8343c467e32a'
        )"""
        query_result = flipside.query(sql)
        return [record['tx_hash'] for record in query_result.records]
    except Exception as e:
        print(f"Error querying Flipside: {e}")
        return []

def get_existing_transactions():
    """Get existing transactions from database"""
    with engine.connect() as connection:
        result = connection.execute(text("SELECT tx_hash FROM floki_transactions"))
        return {row[0] for row in result}

def get_transaction_receipt(tx_hash):
    """Get transaction receipt from Alchemy"""
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    try:
        response = requests.post(ALCHEMY_URL, json=payload, headers=headers)
        return response.json()['result']
    except Exception as e:
        print(f"Error fetching transaction receipt: {e}")
        return None

def decode_token_amounts(logs, tx_receipt):
    """Decode token amounts from transaction logs"""
    try:
    user_address = tx_receipt['from']
        tx_hash = tx_receipt['transactionHash']
    
    wbnb_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'.lower()
        transfer_event = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'.lower()
        
        # Find the main token transfer and WBNB/BNB value
        token_transfer = None
        wbnb_transfer = None
        bnb_value = int(tx_receipt.get('value', '0x0'), 16)  # Get native BNB value
        
        for log in logs:
            if log['topics'][0].lower() == transfer_event:
                if log['address'].lower() == wbnb_address:
                    # Store WBNB transfer
                    wbnb_transfer = log
                else:
                    # Store non-WBNB transfer (the traded token)
                    token_transfer = log
        
        # Handle both WBNB and native BNB cases
        if not token_transfer:
            raise ValueError("Missing token transfer logs")
            
        # Calculate token amount
        raw_token_amount = int(token_transfer['data'], 16)
        input_token = token_transfer['address']
        
        # Calculate denominated amount (WBNB or BNB)
        if wbnb_transfer:
            # WBNB case
            raw_denominated_amount = int(wbnb_transfer['data'], 16)
            is_buy = wbnb_transfer['topics'][1].lower().endswith(user_address.lower()[2:])
    else:
            # Native BNB case
            raw_denominated_amount = bnb_value
            is_buy = bnb_value > 0  # If BNB value exists, it's a buy
        
        denominated_amount = Decimal(raw_denominated_amount) / Decimal(10**18)
        
        # Get token decimals from database
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT decimals 
                FROM token_contract 
                WHERE contract_address = :address
            """), {'address': input_token.lower()})
            token_decimals = result.scalar() or 18
            
        # Calculate actual token amount using correct decimals
        token_amount = Decimal(raw_token_amount) / Decimal(10**token_decimals)
    
    return {
            'tx_hash': tx_hash,
            'transaction_type': 'BUY' if is_buy else 'SELL',
        'user_address': user_address,
            'block_timestamp': datetime.fromtimestamp(w3.eth.get_block(int(tx_receipt['blockNumber'], 16))['timestamp']),
            'traded_token': input_token,
            'token_amount': float(token_amount),
            'denominated_amount': float(denominated_amount),
            'blockchain': 'BNB Chain'
        }
    except Exception as e:
        print(f"Error processing transaction {tx_hash}: {e}")
        raise

async def get_transaction_receipt_async(session, tx_hash):
    """Get transaction receipt from Alchemy asynchronously"""
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
    try:
        async with session.post(ALCHEMY_URL, json=payload, headers=headers) as response:
            result = await response.json()
                if 'result' in result:
            return result['result']
                elif 'error' in result:
                    print(f"API error for {tx_hash}: {result['error']}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                    return None
    except Exception as e:
        print(f"Error fetching transaction receipt for {tx_hash}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            return None
        return None

async def process_batch(session, batch_tx_hashes):
    """Process a batch of transactions concurrently"""
    tasks = []
    for tx_hash in batch_tx_hashes:
        tasks.append(get_transaction_receipt_async(session, tx_hash))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if isinstance(r, dict) and r is not None]

def get_existing_tokens():
    """Get existing tokens from database"""
    with engine.connect() as connection:
        result = connection.execute(text("SELECT contract_address FROM token_contract"))
        return {row[0].lower() for row in result}

def get_token_info(token_address):
    """Get token info using Alchemy API"""
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenMetadata",
        "params": [token_address]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    try:
        response = requests.post(ALCHEMY_URL, json=payload, headers=headers)
        data = response.json()
        
        if 'result' in data:
            name = data['result'].get('name', 'Unknown')
            symbol = data['result'].get('symbol', 'Unknown')
            
            # Skip if symbol is longer than 20 characters
            if len(symbol) > 20:
                print(f"Error: Symbol '{symbol}' exceeds 20 characters for {token_address}")
                return None
                
            return {
                'contract_address': token_address,
                'token_name': name[:255],  # Limit name length to 255 chars
                'token_symbol': symbol,
                'decimals': data['result'].get('decimals', 18),
                'blockchain': 'BNB Chain'
            }
    except Exception as e:
        print(f"Error fetching metadata for {token_address}: {str(e)[:100]}")
        return None

async def process_missing_transactions_async(missing_tx_hashes):
    """Process missing transactions asynchronously"""
    print(f"Starting to process {len(missing_tx_hashes)} transactions...")
    results = []
    
    # Updated optimized settings
    batch_size = 30  # 30 requests per batch
    max_concurrent = 50  # Slightly higher than rate limit
    requests_per_second = 30
    
    # Configure session with optimized settings
    connector = aiohttp.TCPConnector(limit=0,  # Unlimited connections
                                    limit_per_host=30,  # Match rate limit
                                    ttl_dns_cache=300,
                                    force_close=False)
    timeout = aiohttp.ClientTimeout(total=2.0)  # More aggressive timeout
    
    # Get existing tokens once at start
    existing_tokens = get_existing_tokens()
    missing_tokens = set()

    async with aiohttp.ClientSession(connector=connector, 
                                    timeout=timeout,
                                    trust_env=True) as session:
        # Process in exact batch sizes
        for i in tqdm(range(0, len(missing_tx_hashes), batch_size)):
            batch_start = time.monotonic()
            batch = list(missing_tx_hashes)[i:i + batch_size]
            
                # Process batch
                batch_results = await process_batch(session, batch)
            
            # Enforce EXACT 1-second interval between batch starts
            batch_duration = time.monotonic() - batch_start
            sleep_time = max(0, 1.0 - batch_duration)
            await asyncio.sleep(sleep_time)
            
            # Process results
            if batch_results:
                try:
                for tx_receipt in batch_results:
                    if tx_receipt and tx_receipt.get('logs'):
                        try:
                            result = decode_token_amounts(tx_receipt['logs'], tx_receipt)
                            results.append(result)
                            
                                # Check if token exists
                                token_address = result['traded_token'].lower()
                                if token_address not in existing_tokens:
                                    missing_tokens.add(token_address)
                                
                        except Exception as e:
                                print(f"Error processing transaction {tx_receipt['transactionHash']}: {e}")
                                continue
            except Exception as e:
                print(f"Error processing batch: {e}")
            
            # Process missing tokens
            if missing_tokens:
                print(f"Found {len(missing_tokens)} new tokens to add")
                new_tokens = []
                for token_address in missing_tokens:
                    token_info = get_token_info(token_address)
                    if token_info:
                        new_tokens.append(token_info)
                
                if new_tokens:
                    try:
                        df_tokens = pd.DataFrame(new_tokens)
                        df_tokens.to_sql('token_contract', 
                                       engine, 
                                       if_exists='append', 
                                       index=False,
                                       dtype={
                                           'contract_address': String(42),
                                           'token_name': String(255),
                                           'token_symbol': String(20),
                                           'decimals': Integer
                                       })
                        print(f"Added {len(df_tokens)} new tokens to database")
                        existing_tokens.update(missing_tokens)
                        missing_tokens.clear()
                    except IntegrityError:
                        print("Some tokens already existed in database")
                    except Exception as e:
                        print(f"Error saving tokens: {e}")

            # Save results if we have enough
            if len(results) >= 30:
                try:
                    df = pd.DataFrame(results)
                    print(f"Saving {len(df)} transactions to database")
                    
                    # Add transaction hash logging for errors
                    for _, row in df.iterrows():
                        try:
                            # Convert to dictionary for single row insertion
                            row.to_frame().T.to_sql(
                                'floki_transactions',
                                engine,
                                if_exists='append',
                                index=False,
                                method='multi',
                                dtype={
                                    'token_amount': Numeric(precision=78, scale=18),
                                    'denominated_amount': Numeric(precision=78, scale=18)
                                }
                            )
                        except Exception as e:
                            print(f"Error saving transaction {row['tx_hash']}: {e}")
                            continue  # Skip this transaction but continue with others
                    
                    print(f"Successfully saved {len(df)} transactions")
                    results = []
                except Exception as e:
                    print(f"Database error: {e}")
                    # Log all problematic hashes
                    if 'df' in locals():
                        print("Problematic transactions:")
                        for _, row in df.iterrows():
                            print(f"- {row['tx_hash']}")
    
    # Save any remaining results
    if results:
        try:
        df = pd.DataFrame(results)
            print(f"Saving final {len(df)} transactions to database")
            df.to_sql('floki_transactions', 
                    engine, 
                    if_exists='append', 
                    index=False,
                    method='multi',
                    dtype={
                        'token_amount': Numeric(precision=78, scale=18),
                        'denominated_amount': Numeric(precision=78, scale=18)
                    })
        except Exception as e:
            print(f"Error saving final batch to database: {e}")

    print("Finished processing all transactions")

def get_dune_transactions():
    """Get transactions directly from Dune Analytics"""
    try:
        dune = DuneClient(DUNE_API_KEY)
        response = dune.get_custom_endpoint_result(
            "flokiwork",
            "fetching-data"
        )
        return [row['tx_hash'].lower() for row in response.result.rows]
    except Exception as e:
        print(f"Error fetching Dune transactions: {e}")
        return []

def update_token_metadata():
    """Update token metadata for any missing tokens in the token_contract table"""
    print("Updating token metadata...")
    
    # Get all unique contract addresses from floki_transactions that aren't in token_contract
    query = """
    SELECT DISTINCT traded_token 
    FROM floki_transactions ft
    WHERE NOT EXISTS (
        SELECT 1 FROM token_contract tc 
        WHERE tc.contract_address = ft.traded_token
    )
    """
    
    try:
        with engine.connect() as conn:
            addresses = pd.read_sql_query(query, conn)
            
        print(f"Found {len(addresses)} tokens needing metadata updates")
        
        # Update metadata for each new token
        for contract_address in addresses['traded_token']:
            token_info = get_token_info(contract_address)
            
            if token_info is None:
                print(f"Skipping {contract_address} due to validation failure")
                continue
                
            try:
                with engine.connect() as conn:
                    insert_query = text("""
                        INSERT INTO token_contract 
                            (contract_address, token_name, token_symbol, decimals, blockchain)
                        VALUES 
                            (:address, :name, :symbol, :decimals, :blockchain)
                        ON CONFLICT DO NOTHING
                    """)
                    
                    conn.execute(insert_query, {
                        'address': token_info['contract_address'],
                        'name': token_info['token_name'],
                        'symbol': token_info['token_symbol'],
                        'decimals': token_info['decimals'],
                        'blockchain': token_info['blockchain']
                    })
                    conn.commit()
                    
                print(f"Added new token {contract_address}: {token_info['token_name']} ({token_info['token_symbol']})")
            except Exception as e:
                print(f"Error inserting token {contract_address}: {e}")
                
    except Exception as e:
        print(f"Error updating token metadata: {e}")

def upload_to_dune():
    """Upload the final dataframes to Dune Analytics"""
    print("Uploading data to Dune Analytics...")
    
    # Initialize Dune client
    DUNE_API_KEY = "dvninAFuE0Sl4DN1qNYIUQ6Q1ooKDRaR"
    dune = DuneClient(DUNE_API_KEY)
    
    try:
        # Clear existing data from tables
        headers = {
            "X-DUNE-API-KEY": DUNE_API_KEY
        }
        
        # Clear transactions table
        print("Clearing existing transactions data...")
        transactions_clear_url = "https://api.dune.com/api/v1/table/flokiwork/floki_transactions/clear"
        response = requests.post(transactions_clear_url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to clear transactions table: {response.text}")
        
        # Clear token contract table
        print("Clearing existing token contract data...")
        token_clear_url = "https://api.dune.com/api/v1/table/flokiwork/token_contract/clear"
        response = requests.post(token_clear_url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to clear token contract table: {response.text}")
        
        # Get transactions data
        transactions_query = "SELECT tx_hash, transaction_type, user_address, block_timestamp, traded_token, token_amount, denominated_amount, blockchain FROM floki_transactions"
        transactions_df = pd.read_sql_query(transactions_query, engine)
        
        # Save transactions to CSV
        transactions_csv = "floki_transactions.csv"
        transactions_df.to_csv(transactions_csv, index=False)
        
        # Upload transactions
        print("Uploading transactions data...")
        with open(transactions_csv, "rb") as data:
            response = dune.insert_table(
                namespace="flokiwork",
                table_name="floki_transactions",
                data=data,
                content_type="text/csv"
            )
        print("Transactions upload response:", response)
        
        # Get token contract data
        token_query = "SELECT contract_address, token_name, blockchain FROM token_contract"
        token_df = pd.read_sql_query(token_query, engine)
        
        # Save token data to CSV
        token_csv = "token_contract.csv"
        token_df.to_csv(token_csv, index=False)
        
        # Upload token contract data
        print("Uploading token contract data...")
        with open(token_csv, "rb") as data:
            response = dune.insert_table(
                namespace="flokiwork",
                table_name="token_contract",
                data=data,
                content_type="text/csv"
            )
        print("Token contract upload response:", response)
        
        print("Data upload to Dune completed successfully")
        
    except Exception as e:
        print(f"Error uploading to Dune: {e}")
    
    finally:
        # Clean up CSV files
        import os
        for file in [transactions_csv, token_csv]:
            if os.path.exists(file):
                os.remove(file)

def main():
    print("Initializing database schema...")
    initialize_database_schema()
    
    print("Collecting transactions from all sources...")
    
    # Get transactions from all sources
    flipside_tx = [h.lower() for h in get_flipside_transactions()]
    dune_tx = get_dune_transactions()
    
    # Combine and deduplicate
    all_tx_hashes = list(set(flipside_tx + dune_tx))
    print(f"Total unique transactions from all sources: {len(all_tx_hashes)}")
    
    # Existing database check remains the same
    print("Getting existing transactions from database...")
    existing_tx_hashes = get_existing_transactions()
    print(f"Found {len(existing_tx_hashes)} existing transactions in database")
    
    missing_tx_hashes = set(all_tx_hashes) - existing_tx_hashes
    print(f"Found {len(missing_tx_hashes)} missing transactions")
    
    if missing_tx_hashes:
        print("Processing missing transactions...")
        asyncio.run(process_missing_transactions_async(missing_tx_hashes))
        update_token_metadata()
        
        # Add Dune upload at the end
        upload_to_dune()
    else:
        print("No missing transactions to process")

if __name__ == "__main__":
    main() 