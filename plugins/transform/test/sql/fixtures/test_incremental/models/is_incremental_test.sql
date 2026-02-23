SELECT * FROM raw_events
-- __is_incremental__
WHERE id > (SELECT MAX(id) FROM __this__)
-- __end_incremental__
