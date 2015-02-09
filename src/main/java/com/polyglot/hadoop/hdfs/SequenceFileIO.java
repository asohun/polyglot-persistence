package com.polyglot.hadoop.hdfs;

public interface SequenceFileIO {

	void setConfiguration();
	
	void setKeyClazz();

	void setValueClazz();

}
