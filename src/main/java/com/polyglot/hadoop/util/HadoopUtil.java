package com.polyglot.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtil {

	private static final Logger log = LoggerFactory.getLogger(HadoopUtil.class);

	public static void main(String[] args) {
		getHBaseConfiguration();
	}

	public static Configuration getHBaseConfiguration() {
		Configuration configuration = new Configuration();
		configuration.clear();
		//configuration.set("hbase.zookeeper.quorum", Constant.IP_SERVER);
		//configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.master", Constant.IP_SERVER + ":60010");

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

	public static Configuration getHDFSConfiguration() {
		Configuration configuration = new Configuration();
		configuration.clear();
		// configuration.addResource(new Path("core-site.xml"));
		configuration.set("fs.defaultFS", "hdfs://" + Constant.IP_SERVER + ":"
				+ Constant.PORT_NAMENODE);
		configuration.set("hadoop.tmp.dir", "/app/hadoop/tmp");
		configuration.set("hadoop.home.dir", "/usr/local/hadoop");
		return configuration;
	}
}
