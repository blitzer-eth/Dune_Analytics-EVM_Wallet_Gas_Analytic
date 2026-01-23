WITH input_wallet AS (
  SELECT FROM_HEX(SUBSTRING(LOWER('{{Wallet Address}}'), 3)) AS wallet
),

time_filter AS (
  SELECT 
    CASE 
      WHEN '{{Time Period}}' = 'Past Week'     THEN CURRENT_DATE - INTERVAL '7' day
      WHEN '{{Time Period}}' = 'Past Month'    THEN CURRENT_DATE - INTERVAL '1' month
      WHEN '{{Time Period}}' = 'Past 6 Months' THEN CURRENT_DATE - INTERVAL '6' month
      WHEN '{{Time Period}}' = 'Past Year'     THEN CURRENT_DATE - INTERVAL '1' year
      WHEN '{{Time Period}}' = 'All Time'      THEN CAST('2015-01-01' AS DATE) -- The beginning of Ethereum
    END AS start_date
),

eth_gas_chains AS (
  SELECT * FROM (
    VALUES
      ('ethereum'),
      ('arbitrum'),
      ('optimism'),
      ('base'), 
      ('blast'),
      ('linea'),
      ('scroll'),
      ('zksync'), 
      ('zora'),
      ('zkevm'),
      ('mode')
  ) AS t(blockchain)
),

daily_by_chain AS (
  SELECT
    DATE_TRUNC('day', f.block_time) AS day,
    f.blockchain,
    SUM(f.tx_fee) AS daily_gas_eth
  FROM gas.fees f
  JOIN input_wallet iw ON f.tx_from = iw.wallet
  JOIN eth_gas_chains c ON f.blockchain = c.blockchain
  CROSS JOIN time_filter tf
  WHERE f.block_time >= tf.start_date
  GROUP BY 1, 2
),

chain_series AS (
  SELECT
    day,
    blockchain,
    daily_gas_eth,
    SUM(daily_gas_eth) OVER (
      PARTITION BY blockchain
      ORDER BY day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_chain_eth
  FROM daily_by_chain
),

overall_series AS (
  SELECT
    day,
    SUM(daily_gas_eth) AS daily_total_eth,
    SUM(SUM(daily_gas_eth)) OVER (
      ORDER BY day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_total_eth
  FROM daily_by_chain
  GROUP BY 1
),

chain_with_shares AS (
  SELECT
    c.day,
    c.blockchain,
    c.daily_gas_eth,
    o.daily_total_eth,
    CASE
      WHEN o.daily_total_eth = 0 THEN 0
      ELSE c.daily_gas_eth / o.daily_total_eth
    END AS daily_share,
    c.cumulative_chain_eth
  FROM chain_series c
  JOIN overall_series o ON c.day = o.day
),

overall_row AS (
  SELECT
    day,
    'ALL' AS blockchain,
    CAST(NULL AS DOUBLE) AS daily_gas_eth,
    daily_total_eth,
    CAST(NULL AS DOUBLE) AS daily_share,
    CAST(NULL AS DOUBLE) AS cumulative_chain_eth,
    cumulative_total_eth
  FROM overall_series
),

total_all_time AS (
  SELECT
    SUM(daily_gas_eth) AS total_gas_eth_all_time
  FROM daily_by_chain
),

final_union AS (
  SELECT
    day, blockchain, daily_gas_eth, daily_total_eth, daily_share, cumulative_chain_eth,
    CAST(NULL AS DOUBLE) AS cumulative_total_eth
  FROM chain_with_shares
  UNION ALL
  SELECT
    day, blockchain, daily_gas_eth, daily_total_eth, daily_share, cumulative_chain_eth,
    cumulative_total_eth
  FROM overall_row
)

SELECT
  f.day,
  f.blockchain,
  CAST(f.daily_gas_eth AS DECIMAL(18, 8)) AS daily_gas_eth,
  CAST(f.daily_total_eth AS DECIMAL(18, 8)) AS daily_total_eth,
  CAST(f.daily_share AS DECIMAL(18, 6)) AS daily_share,
  CAST(f.cumulative_chain_eth AS DECIMAL(18, 8)) AS cumulative_chain_eth,
  CAST(f.cumulative_total_eth AS DECIMAL(18, 8)) AS cumulative_total_eth,
  CAST(t.total_gas_eth_all_time AS DOUBLE) AS total_gas_eth_all_time_num,
  CAST(t.total_gas_eth_all_time AS DECIMAL(18, 8)) AS total_gas_eth_all_time
FROM final_union f
CROSS JOIN total_all_time t
ORDER BY
  f.day,
  CASE WHEN f.blockchain = 'ALL' THEN 0 ELSE 1 END,
  f.daily_gas_eth DESC,
  f.blockchain;
