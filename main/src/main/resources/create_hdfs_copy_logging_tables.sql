CREATE TABLE IF NOT EXISTS hdfscopy_result(
      dst_path string,
      action string,
      src_path string,
      size bigint,
      ts bigint)
PARTITIONED BY (
      jobts bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
