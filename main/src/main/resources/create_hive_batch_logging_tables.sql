-- Stage 1 job output table.
CREATE TABLE IF NOT EXISTS hivecopy_stage1_result(
      action string,    -- action for the entity
      srcpath string,   -- source entity path
      dstpath string,   -- destination enitity path
      copydata string,  -- should copy data
      copymetadata string, -- should copy metadata
      db string,           -- database name
      tbl string,          -- table name
      part string)         -- partition spec
PARTITIONED BY (
      jobts bigint)        -- job run timestamp
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

-- Stage 2 job output table.
CREATE TABLE IF NOT EXITS hivecopy_stage2_result(
      action string,   -- action for stage 2, copied or skipped
      srcpath string,  -- hdfs file source path
      dstpath string,  -- hdfs file destination path
      size string,     -- file size
      extra string,    -- extra error logging information
      time string)     -- copy action time
PARTITIONED BY (
      jobts bigint)    -- job run timestamp
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

-- Stage 3 job output table.
CREATE TABLE IF NOT EXITS hivecopy_stage3_result(
      action string,
      srcpath string,
      dstpath string,
      copydata string,
      copymetadata string,
      db string,
      tbl string,
      part string,
      extra string)    -- extra information for commit phase
PARTITIONED BY (
      jobts bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
