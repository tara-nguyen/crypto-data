SELECT
	DATE(time),
	network,
	name "type",
	total "tokenAmount",
	account_count "holderCount",
	total - total_without_system "systemToken",
	account_count - account_count_without_system "systemAccounts"
FROM subscan_asset_statistics
WHERE time IN (
	SELECT MAX(time)
	FROM subscan_asset_statistics
	WHERE time < '2023-07-13'
	GROUP BY network
)
ORDER BY network,
CASE
	WHEN name = 'Whale' THEN 1
	WHEN name = 'Dolphin' THEN 2
	WHEN name = 'Fish' THEN 3
	ELSE 4
END