package com.polyglot.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.logging.Logger;

import com.polyglot.hadoop.util.HadoopUtil;

public class TestHBase {

	private final Logger log = Logger.getLogger(TestHDFS.class);

	private Configuration configuration;

	public TestHBase() {
		configuration = HadoopUtil.getConfiguration();
	}

	public HTableInterface createTable(String tableName) throws IOException {
		return new HTable(configuration, tableName);
	}

	public HTableInterface getTable(String tableName, String pool)
			throws IOException {
		int mSize = 9;
		HTablePool _tPool = new HTablePool(configuration, mSize);
		HTableInterface table = _tPool.getTable(tableName);
		_tPool.putTable(table);
		_tPool.closeTablePool(pool);

		return table;
	}

	public void get(String tableName, String key, String family) throws IOException {
		HTableInterface table = createTable(tableName);
		Get get = new Get(Bytes.toBytes(key));
		Result result = table.get(get);
		NavigableMap<byte[], byte[]> familyValues = result.getFamilyMap(Bytes
				.toBytes(family));
		for (Map.Entry<byte[], byte[]> entry : familyValues.entrySet()) {
			String column = Bytes.toString(entry.getKey());
			byte[] value = entry.getValue();

			log.info(column + " - " + value);
		}
	}

	public void put(String tableName, String key, String family) throws IOException {
		Map<String, byte[]> rows = null;
		HTable table = new HTable(configuration, tableName);
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
