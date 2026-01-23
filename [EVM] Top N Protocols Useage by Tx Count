WITH input_wallet AS (
  SELECT FROM_HEX(SUBSTRING(LOWER('{{Wallet Address}}'), 3)) AS wallet
),

included_list AS (
    SELECT trim(ch) as blockchain 
    FROM UNNEST(SPLIT('{{Blockchain:}}', ',')) AS t(ch)
),

time_filter AS (
  SELECT 
    CASE 
      WHEN '{{Time Period}}' = 'Past Week'     THEN CURRENT_DATE - INTERVAL '7' day
      WHEN '{{Time Period}}' = 'Past Month'    THEN CURRENT_DATE - INTERVAL '1' month
      WHEN '{{Time Period}}' = 'Past 6 Months' THEN CURRENT_DATE - INTERVAL '6' month
      WHEN '{{Time Period}}' = 'Past Year'     THEN CURRENT_DATE - INTERVAL '1' year
      WHEN '{{Time Period}}' = 'All Time'      THEN CAST('2015-01-01' AS DATE)
    END AS start_date
),

eth_chains AS (
  SELECT blockchain FROM (
    VALUES
      ('ethereum'), ('arbitrum'), ('optimism'), ('base'), ('blast'),
      ('linea'), ('scroll'), ('zksync'), ('zora'), ('zkevm'), ('mode')
  ) AS t(blockchain)
  WHERE blockchain IN (SELECT blockchain FROM included_list)
),

current_eth_price AS (
    SELECT price FROM prices.usd 
    WHERE symbol = 'WETH' AND blockchain = 'ethereum'
    ORDER BY minute DESC LIMIT 1
),

contract_labels AS (
    SELECT 
        f.tx_to AS contract_address,
        f.blockchain,
        COALESCE(evm.namespace || ': ' || evm.name, lbl.name, CAST(f.tx_to AS VARCHAR)) AS protocol_name
    FROM gas.fees f
    LEFT JOIN evms.contracts evm ON evm.address = f.tx_to AND evm.blockchain = f.blockchain
    LEFT JOIN labels.addresses lbl ON lbl.address = f.tx_to AND lbl.blockchain = f.blockchain
    JOIN input_wallet iw ON f.tx_from = iw.wallet
    GROUP BY 1, 2, 3
),

protocol_agg AS (
    SELECT
        cl.protocol_name,
        f.blockchain,
        COUNT(*) AS tx_count,
        SUM(f.tx_fee) AS total_gas_eth,
        SUM(f.tx_fee * p.price) AS total_usd_at_tx,
        ANY_VALUE(f.tx_to) as most_used_address
    FROM gas.fees f
    JOIN input_wallet iw ON f.tx_from = iw.wallet
    JOIN contract_labels cl ON f.tx_to = cl.contract_address AND f.blockchain = cl.blockchain
    JOIN eth_chains ec ON f.blockchain = ec.blockchain
    CROSS JOIN time_filter tf
    LEFT JOIN prices.usd p ON p.minute = DATE_TRUNC('minute', f.block_time)
        AND p.symbol = 'WETH'
        AND p.blockchain = 'ethereum'
    WHERE f.block_time >= tf.start_date
    GROUP BY 1, 2
)

SELECT
    protocol_name,
    blockchain,
    tx_count,
    CAST(total_gas_eth AS DECIMAL(18, 8)) AS total_gas_eth,
    CAST(total_usd_at_tx AS DECIMAL(18, 2)) AS usd_spent_then,
    CAST(total_gas_eth * (SELECT price FROM current_eth_price) AS DECIMAL(18, 2)) AS usd_value_now,
    '<a href="' || 
    CASE 
        WHEN blockchain = 'ethereum' THEN 'https://etherscan.io/address/'
        WHEN blockchain = 'arbitrum' THEN 'https://arbiscan.io/address/'
        WHEN blockchain = 'optimism' THEN 'https://optimistic.etherscan.io/address/'
        WHEN blockchain = 'base'     THEN 'https://basescan.org/address/'
        WHEN blockchain = 'blast'    THEN 'https://blastscan.io/address/'
        WHEN blockchain = 'linea'    THEN 'https://lineascan.build/address/'
        WHEN blockchain = 'scroll'   THEN 'https://scrollscan.com/address/'
        WHEN blockchain = 'zksync'   THEN 'https://explorer.zksync.io/address/'
        WHEN blockchain = 'zora'     THEN 'https://explorer.zora.energy/address/'
        WHEN blockchain = 'zkevm'    THEN 'https://zkevm.polygonscan.com/address/'
        WHEN blockchain = 'mode'     THEN 'https://modescan.io/address/'
        ELSE 'https://dune.com/queries/' 
    END || CAST(most_used_address AS VARCHAR) || '" target="_blank">📄 View Contract</a>' AS contract_link
FROM protocol_agg
ORDER BY tx_count DESC
LIMIT {{Top N:}};
