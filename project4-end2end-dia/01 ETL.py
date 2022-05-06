# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE...

# COMMAND ----------

# Import the necessary libraries
import pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ETL Pipeline to generate tokens_silver_table

# COMMAND ----------

# Read bronze data sources
tokens_bronze_table = spark.table('ethereumetl.tokens')
token_prices_usd_bronze_table = spark.table('ethereumetl.token_prices_usd')
 
# Schema validation
assert tokens_bronze_table.schema == pyspark.sql.types._parse_datatype_string(
    '''
    address: string,
    symbol: string,
    name: string,
    decimals: bigint,
    total_supply: decimal(38, 0),
    start_block: bigint,
    end_block: bigint
    '''
), 'Schema validation for tokens_bronze_table failed'
print('Schema validation for tokens_bronze_table succeeded')
 
assert token_prices_usd_bronze_table.schema == pyspark.sql.types._parse_datatype_string(
    '''
    id: string,
    symbol: string,
    name: string,
    asset_platform_id: string,
    description: string,
    links: string,
    image: string,
    contract_address: string,
    sentiment_votes_up_percentage: double,
    sentiment_votes_down_percentage: double,
    market_cap_rank: double,
    coingecko_rank: double,
    coingecko_score: double,
    developer_score: double,
    community_score: double,
    liquidity_score: double,
    public_interest_score: double,
    price_usd: double
    '''
), 'Schema validation for token_prices_usd_bronze_table failed'
print('Schema validation for token_prices_usd_bronze_table succeeded')
 
# Bronze-to-Silver transformation
# 1. Inner join between tokens_bronze_table and token_prices_usd_bronze_table on contract_address
# 2. Extract only rows where asset_platform_id is ethereum
# 3. Select only the relevant columns
# 4. Drop duplicates on address column
# 5. Correct the ID column after performing a join operation
# 6. Repartition on address to give a hash-partitioned dataframe
tokens_silver_table = token_prices_usd_bronze_table \
    .join(tokens_bronze_table, (tokens_bronze_table.address == token_prices_usd_bronze_table.contract_address), 'inner') \
    .where(token_prices_usd_bronze_table.asset_platform_id == 'ethereum') \
    .select(
        col('ethereumetl.token_prices_usd.contract_address').alias('address'),
        col('ethereumetl.tokens.name'),
        col('ethereumetl.tokens.symbol'),
        col('ethereumetl.token_prices_usd.price_usd')) \
    .dropDuplicates(['address']) \
    .withColumn('id', row_number().over(pyspark.sql.window.Window.orderBy(lit(1)))) \
    .repartition('address')
 
# Write the tokens_silver_table into the group's database
tokens_silver_table \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('address') \
    .saveAsTable('g07_db.tokens_silver')
 
# Perform z-ordering optimization
spark.sql('OPTIMIZE g07_db.tokens_silver ZORDER BY address')
 
print('ETL Pipeline to generate tokens_silver_table succeeded')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ETL Pipeline for wallet_addresses_table

# COMMAND ----------

# Read bronze data sources
blocks_bronze_table = spark.table('ethereumetl.blocks')
token_transfers_bronze_table = spark.table('ethereumetl.token_transfers')
 
# Schema validation
assert blocks_bronze_table.schema == pyspark.sql.types._parse_datatype_string(
    '''
    number: long,
    hash: string,
    parent_hash: string,
    nonce: string,
    sha3_uncles: string,
    logs_bloom: string,
    transactions_root: string,
    state_root: string,
    receipts_root: string,
    miner: string,
    difficulty: decimal(38, 0),
    total_difficulty: decimal(38, 0),
    size: long,
    extra_data: string,
    gas_limit: long,
    gas_used: long,
    timestamp: long,
    transaction_count: long,
    start_block: long,
    end_block: long
    '''
), 'Schema validation for blocks_bronze_table failed'
print('Schema validation for blocks_bronze_table succeeded')
 
assert token_transfers_bronze_table.schema == pyspark.sql.types._parse_datatype_string(
    '''
    token_address: string,
    from_address: string,
    to_address: string,
    value: decimal(38, 0),
    transaction_hash: string,
    log_index: long,
    block_number: long,
    start_block: long,
    end_block: long
    '''
), 'Schema validation for token_transfers_bronze_table failed'
print('Schema validation for token_transfers_bronze_table succeeded')
 
# Bronze-to-Silver transformation
# 1. Inner join between tokens_silver_table and token_transfers_bronze_table on token_address
# 2. Inner join between blocks_bronze_table and token_transfers_bronze_table on block_number
# 3. Select only the relevant columns
# 4. Timestamp formatting for the timestamp column
# 5. Repartition on ID to give a hash-partitioned dataframe
tokens_transfers_silver_table = token_transfers_bronze_table \
    .join(tokens_silver_table, (tokens_silver_table.address == token_transfers_bronze_table.token_address), 'inner') \
    .join(blocks_bronze_table, (blocks_bronze_table.number == token_transfers_bronze_table.block_number), 'inner') \
    .select('id', 'to_address', 'from_address', 'value', 'timestamp') \
    .withColumn('timestamp', col('timestamp').cast('timestamp')) \
    .repartition('id')
 
print('Bronze-to-Silver transformation for token_transfers_bronze_table succeeded')
 
# Extract the purchased tokens from tokens_transfers_silver_table
 
# Transformation
# 1. GroupBy the ID and to_address
# 2. Add the value column
# 3. Repartition on ID to give a hash-partitioned dataframe
purchased_tokens_table = tokens_transfers_silver_table \
    .groupBy('id', 'to_address') \
    .agg(sum('value').alias('total_buy_value')) \
    .repartition('id')
 
print('Purchased tokens transformation for purchased_tokens_table succeeded')
 
# Extract the sold tokens from tokens_transfers_silver_table
 
# Transformation
# 1. GroupBy the ID and from_address
# 2. Add the value column
# 3. Repartition on ID to give a hash-partitioned dataframe
sold_tokens_table = tokens_transfers_silver_table \
    .groupBy('id', 'from_address') \
    .agg(sum('value').alias('total_sell_value')) \
    .repartition('id')
 
print('Sold tokens transformation for sold_tokens_table succeeded')
 
# Compute the balance value for purchased & sold tokens from purchased_tokens_table and sold_tokens_table
 
# Transformation
# 1. Join the 2 tables for matching token IDs and corresponding addresses
# 2. Compute the balance
# 3. Rename and select the appropriate columns
# 4. Repartition on ID to give a hash-partitioned dataframe
balance_tokens_table = purchased_tokens_table \
    .join(sold_tokens_table, (purchased_tokens_table.id == sold_tokens_table.id) & (purchased_tokens_table.to_address == sold_tokens_table.from_address), 'inner') \
    .withColumn('balance', purchased_tokens_table.total_buy_value - sold_tokens_table.total_sell_value) \
    .withColumnRenamed('to_address', 'address') \
    .select('address', purchased_tokens_table.id, 'balance') \
    .repartition('id')
 
print('Balance value transformation for balance_tokens_table succeeded')
 
# Compute the USD balance value for purchased & sold tokens using balance_tokens_table and tokens_silver_table
 
# Transformation
# 1. Join the 2 tables for matching token IDs
# 2. Compute the USD balance
# 3. Select the appropriate columns
# 4. Repartition on ID to give a hash-partitioned dataframe
used_balance_tokens_table = balance_tokens_table \
    .join(tokens_silver_table, balance_tokens_table.id == tokens_silver_table.id, 'inner') \
    .withColumn('balance_usd', balance_tokens_table.balance * tokens_silver_table.price_usd) \
    .select(balance_tokens_table.address, balance_tokens_table.id, 'balance_usd')\
    .repartition('id')
 
print('USD balance value transformation for used_balance_tokens_table succeeded')
 
# Contruct the wallet_addresses_table by assigning IDs for unique addresses
 
# Transformation
# 1. Select unique addresses
# 2. Assign an ID for each unique address
wallet_addresses_table = used_balance_tokens_table \
    .select('address').distinct() \
    .withColumn('address_id', row_number().over(pyspark.sql.window.Window.orderBy(lit(1))))
 
# Write the wallet_addresses_table into the group's database
wallet_addresses_table \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('g07_db.wallet_addresses')
 
# Perform z-ordering optimization
spark.sql('OPTIMIZE g07_db.wallet_addresses ZORDER BY address')
 
print('ETL Pipeline to generate wallet_addresses_table succeeded')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ETL Pipeline for final_modelling_table

# COMMAND ----------

# Contruct the final_modelling_table by selecting the relevant features for modelling
 
# Transformation
# 1.Join used_balance_tokens_table and wallet_addresses_table on address
# 2. Select and rename the relevant columns
final_modelling_table = used_balance_tokens_table \
    .join(wallet_addresses_table, used_balance_tokens_table.address == wallet_addresses_table.address, 'inner') \
    .select(wallet_addresses_table.address_id, used_balance_tokens_table.id, 'balance_usd')\
    .withColumnRenamed('id', 'token_id')
 
# Write the final_modelling_table into the group's database
final_modelling_table \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('g07_db.final_modelling')
 
# Perform z-ordering optimization
spark.sql('OPTIMIZE g07_db.final_modelling ZORDER BY address_id')
 
print('ETL Pipeline to generate final_modelling_table succeeded')

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({'exit_code': 'OK'}))
