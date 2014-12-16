package com.polyglot.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.jboss.logging.Logger;

public class HadoopUtil {

	private final static Logger log = Logger.getLogger(HadoopUtil.class);

	public static Configuration getConfiguration() {
		Configuration configuration = new Configuration();
		configuration.clear();
		configuration.set("hbase.zookeeper.quorum", Constant.SERVER_IP);
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.master", Constant.SERVER_IP + ":60010");

		try {
			HBaseAdmin.checkHBaseAvailable(configuration);
			log.info("HBase Running");
		} catch (MasterNotRunningException e) {
			log.error("Master Not Running");
		} catch (ZooKeeperConnectionException e) {
			log.error("ZooKeeper Connection Exception");
		}

		return configuration;
	}
}
