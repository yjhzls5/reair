CREATE TABLE IF NOT EXISTS hivecopy_stage1_result(
      action string,
      srcpath string,
      dstpath string,
      copydata string,
      copymetadata string,
      db string,
      tbl string,
      part string)
PARTITIONED BY (
      jobts bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXITS hivecopy_stage2_result(
      action string,
      srcpath string,
      dstpath string,
      size string,
      extra string,
      time string)
PARTITIONED BY (
      jobts bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXITS hivecopy_stage3_result(
      action string,
      srcpath string,
      dstpath string,
      copydata string,
      copymetadata string,
      db string,
      tbl string,
      part string,
      extra string)
PARTITIONED BY (
      jobts bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
