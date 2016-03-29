create database monitor_result;
use monitor_result;

CREATE TABLE `result_info` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `access_key` varchar(200) NOT NULL,
  `stream_url` varchar(200) NOT NULL,
  `stream_id` varchar(200) NOT NULL,
  `result` text NOT NULL,
  `timestamp` datetime NOT NULL,
  `catchDate` date NOT NULL,

  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
