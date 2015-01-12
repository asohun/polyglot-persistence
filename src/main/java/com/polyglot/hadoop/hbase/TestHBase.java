package com.polyglot.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polyglot.hadoop.util.HBaseUtil;

/**
 * @author asohun
 */
public class TestHBase {

	private static final Logger log = LoggerFactory.getLogger(TestHBase.class);

	private Configuration configuration;

	/**
	 * Default constructor
	 */
	public TestHBase() {
		configuration = HBaseUtil.getHBaseConfiguration();
	}
	
	public static void main(String[] args) {
		TestHBase hbase = new TestHBase();
		String operation = args[0];

		if ("create".equals(operation)) {
			hbase.create(args[1]);
		}
	}

	/**
	 * Create HBase table
	 * 
	 * @param tableName
	 *            string representing the name of the table
	 * @return hTableInterface representing the created table
	 */
	public HTableInterface create(String tableName) {
		HTableInterface table = null;
		try {
			table = new HTable(configuration, tableName);
			log.debug("Table " + tableName + " created.");
		} catch (IOException e) {
			log.error(e.getMessage());
		}

		return table;
	}

	/**
	 * Retrieve HBase table
	 * 
	 * @param tableName
	 *            string representing the name of the table
	 * @return hTableInterface representing the retrieved table
	 */
	public HTableInterface getTable(String tableName) {
		HConnection connection = null;
		HTableInterface table = null;

		try {
			connection = HConnectionManager.createConnection(configuration);
			table = connection.getTable(tableName);
			log.debug(table.toString() + " retrieved");
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}

		return table;
	}

	public void get(String tableName, String key, String family) {
		HTableInterface table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(key));
		Result result = null;

		if (table != null) {
			try {
				result = table.get(get);
			} catch (IOException e) {
				log.error(e.getMessage());
			}
		}

		if (result != null) {
			NavigableMap<byte[], byte[]> familyValues = result
					.getFamilyMap(Bytes.toBytes(family));
			for (Map.Entry<byte[], byte[]> entry : familyValues.entrySet()) {
				String column = Bytes.toString(entry.getKey());
				byte[] value = entry.getValue();

				log.info(column + " - " + value);
			}
		}
	}

	public void put(String tableName, String key, String family)
			throws IOException {
		Map<String, byte[]> rows = null;
		HTableInterface table = getTable(tableName);
		List<Put> puts = new ArrayList<Put>();
		for (Map.Entry<String, byte[]> row : rows.entrySet()) {
			byte[] bkey = Bytes.toBytes(row.getKey());
			Put put = new Put(bkey);
			put.add(Bytes.toBytes("family"), Bytes.toBytes("column"),
					row.getValue());
			puts.add(put);
		}
		table.put(puts);
	}

}
