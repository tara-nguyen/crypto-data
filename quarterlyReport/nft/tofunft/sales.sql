SELECT
	network,
	DATE(created_at),
	SUM(price) token
FROM tofunft_trade_data
WHERE category = 'sale'
AND created_at >= '2023-04-01'
AND created_at < '2023-07-01'
GROUP BY 1, 2
ORDER BY 1, 2 DESC