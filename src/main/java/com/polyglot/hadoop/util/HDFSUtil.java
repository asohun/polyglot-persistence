package com.polyglot.hadoop.util;

import org.apache.hadoop.conf.Configuration;

public class HDFSUtil {

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
