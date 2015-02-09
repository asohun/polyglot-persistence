package com.polyglot.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.polyglot.hadoop.util.Constant;

public class IndexDescriptor implements Writable {
	private Long document;
	private int frequency;
	private Map<Long, Long> index;

	public IndexDescriptor() {
		this.index = new HashMap<Long, Long>();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(document);
		out.writeInt(frequency);

		StringBuffer index = new StringBuffer();
		Iterator<Long> iterator = this.index.keySet().iterator();
		while (iterator.hasNext()) {
			Long key = iterator.next();
			Long value = this.index.get(key);
			index.append(key);
			index.append(Constant.DELIMETER_KEY_VALUE);
			index.append(value);
			index.append(Constant.DELIMETER_LINE);
		}

		out.writeChars(index.toString());
	}

	public void readFields(DataInput in) throws IOException {
		document = in.readLong();
		frequency = in.readInt();
		String index = in.readLine();

		String[] lines = index.split(Constant.DELIMETER_LINE);
		if (lines != null) {
			for (String line : lines) {
				String[] keyValue = line.split(Constant.DELIMETER_KEY_VALUE);
				this.index.put(new Long(keyValue[0]), new Long(keyValue[1]));
			}
		}
	}

	public Long getDocument() {
		return document;
	}

	public void setDocument(Long document) {
		this.document = document;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	public Map<Long, Long> getIndex() {
		return index;
	}

	public void setIndex(Map<Long, Long> index) {
		this.index = index;
	}

}
