package com.polyglot.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBase {

	public void test() throws IOException {
		Configuration configuration = new Configuration();
		HTable table = new HTable(configuration, "Table");
	}

	public void test2() throws IOException {
		Configuration configuration = new Configuration();
		int mSize = 9;
		Configuration config = null;
		HTablePool _tPool = new HTablePool(config, mSize);
		HTableInterface table = _tPool.getTable("Table");
		_tPool.putTable(table);
		_tPool.closeTablePool("testtable");
	}

	public void test3() throws IOException {
		Configuration configuration = null;
		HTable table = new HTable(configuration, "table");
		Get get = new Get(Bytes.toBytes("key"));
		Result result = table.get(get);
		NavigableMap<byte[], byte[]> familyValues = result.getFamilyMap(Bytes
				.toBytes("columnFamily"));
		for (Map.Entry<byte[], byte[]> entry : familyValues.entrySet()) {
			String column = Bytes.toString(entry.getKey());
			byte[] value = entry.getValue();
		}
	}

	public void test4(Configuration configuration) throws IOException {
		Map<String, byte[]> rows = null;
		HTable table = new HTable(configuration, "table");
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
