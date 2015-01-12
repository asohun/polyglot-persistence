package com.polyglot.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;

public class HBaseUtil {

	private static final Logger log = LoggerFactory.getLogger(HBaseUtil.class);

	public static Configuration getHBaseConfiguration() {
		Configuration configuration = new Configuration();
		configuration.clear();
		configuration.set("hbase.zookeeper.quorum", Constant.IP_SERVER);
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.master", Constant.IP_SERVER + ":60000");

		try {
			HBaseAdmin.checkHBaseAvailable(configuration);
			log.debug("HBase Running");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			log.error("Master Not Running");
		} catch (ZooKeeperConnectionException e) {
			log.error("ZooKeeper Connection Exception");
		} catch (ServiceException e) {
			log.error("Service Exception");
		} catch (IOException e) {
			log.error("IO Exception");
		}

		return configuration;
	}

}