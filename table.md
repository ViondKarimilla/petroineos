CREATE TABLE energy_supply_quarterly (
  event_date DATE,
  event_year INT,
  event_quarter INT,
  series_name STRING,
  value DOUBLE,
  source_filename STRING,
  source_sheet STRING,
  event_timestamp TIMESTAMP
)
USING ICEBERG
PARTITIONED BY (years(event_date), event_quarter)
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read',
  'write.target-file-size-bytes' = '536870912',
  'read.split.target-size' = '134217728'
);



Upsert Strategy

Use MERGE INTO
which will load from raw table to stageing with matching records only
(Latest record wins based on event_timestamp)

MERGE INTO energy_supply_quarterly t
USING staging_energy_supply s
ON t.event_date = s.event_date
AND t.series_name = s.series_name
WHEN MATCHED AND s.event_timestamp > t.event_timestamp THEN
  UPDATE SET *
WHEN NOT MATCHED THEN

  INSERT *
