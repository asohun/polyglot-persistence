package com.polyglot.hadoop.mapreduce;

import java.util.HashMap;
import java.util.Map;

public class IndexDescriptor {
	private Long document;
	private int frequency;
	private Map<Long, Long> index;

	public IndexDescriptor() {
		this.index = new HashMap<Long, Long>();
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
