-- Create the table containing query information
CREATE TABLE `audit_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `query_id` varchar(256) DEFAULT NULL,
  `command_type` varchar(64) DEFAULT NULL,
  `command` mediumtext,
  `inputs` mediumtext,
  `outputs` mediumtext,
  `username` varchar(64) DEFAULT NULL,
  `chronos_job_name` varchar(256) DEFAULT NULL,
  `chronos_job_owner` varchar(256) DEFAULT NULL,
  `mesos_task_id` varchar(256) DEFAULT NULL,
  `ip` varchar(64) DEFAULT NULL,
  `extras` mediumtext,
  PRIMARY KEY (`id`),
  KEY `create_time_index` (`create_time`)
);

-- This table hold metadata changes as reflected in the Thrift metadata objects
CREATE TABLE `audit_objects` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `audit_log_id` bigint(20) NOT NULL,
  `category` varchar(64) DEFAULT NULL,
  `type` varchar(64) DEFAULT NULL,
  `name` varchar(4000) DEFAULT NULL,
  `serialized_object` mediumtext,
  PRIMARY KEY (`id`),
  KEY `create_time_index` (`create_time`),
  KEY `audit_log_id_index` (`audit_log_id`)
)