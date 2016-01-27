-- hdfs copy job output table.
CREATE TABLE IF NOT EXISTS hdfscopy_result(
      dst_path string,  -- destination path
      action string,    -- action, add, update, delete
      src_path string,  -- source path
      size bigint,      -- size
      ts bigint)        -- file timestamp
PARTITIONED BY (
      jobts bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
