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
    SELECT price 
    FROM prices.usd 
    WHERE symbol = 'WETH' 
    AND blockchain = 'ethereum'
    ORDER BY minute DESC 
    LIMIT 1
)

SELECT
    f.block_time AS date,
    f.blockchain,
    CAST(f.tx_fee AS DECIMAL(18, 8)) AS gas_spent_eth,
    
    -- USD Value at the time of transaction
    CAST(f.tx_fee * p.price AS DECIMAL(18, 2)) AS usd_value_at_tx,
    
    -- USD Value at current market price
    CAST(f.tx_fee * (SELECT price FROM current_eth_price) AS DECIMAL(18, 2)) AS usd_value_now,

    -- Dynamic Explorer Links
    '<a href="' || 
        CASE 
            WHEN f.blockchain = 'ethereum' THEN 'https://etherscan.io/tx/'
            WHEN f.blockchain = 'arbitrum' THEN 'https://arbiscan.io/tx/'
            WHEN f.blockchain = 'optimism' THEN 'https://optimistic.etherscan.io/tx/'
            WHEN f.blockchain = 'base'     THEN 'https://basescan.org/tx/'
            WHEN f.blockchain = 'blast'    THEN 'https://blastscan.io/tx/'
            WHEN f.blockchain = 'linea'    THEN 'https://lineascan.build/tx/'
            WHEN f.blockchain = 'scroll'   THEN 'https://scrollscan.com/tx/'
            WHEN f.blockchain = 'zksync'   THEN 'https://explorer.zksync.io/tx/'
            WHEN f.blockchain = 'zora'     THEN 'https://explorer.zora.energy/tx/'
            WHEN f.blockchain = 'zkevm'    THEN 'https://zkevm.polygonscan.com/tx/'
            WHEN f.blockchain = 'mode'     THEN 'https://modescan.io/tx/'
            ELSE 'https://dune.com/queries/' 
        END || CAST(f.tx_hash AS VARCHAR) || '" target="_blank">🔗 View</a>' AS explorer_link,
    f.tx_hash
FROM gas.fees f
JOIN input_wallet iw ON f.tx_from = iw.wallet
JOIN eth_chains ec ON f.blockchain = ec.blockchain
CROSS JOIN time_filter tf
LEFT JOIN prices.usd p ON p.minute = DATE_TRUNC('minute', f.block_time)
    AND p.symbol = 'WETH'
    AND p.blockchain = 'ethereum'
WHERE f.block_time >= tf.start_date
ORDER BY f.tx_fee DESC
LIMIT {{Top N:}};
