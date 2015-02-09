package com.polyglot.hadoop.hdfs;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polyglot.hadoop.mapreduce.IndexDescriptor;
import com.polyglot.hadoop.util.Constant;
import com.polyglot.hadoop.util.HDFSUtil;

public class IndexDescriptorSequenceFile extends
		BaseSequenceFileIO<Text, IndexDescriptor> {

	private final static Logger log = LoggerFactory
			.getLogger(IndexDescriptorSequenceFile.class);

	public static void main(String[] args) {
		IndexDescriptorSequenceFile hdfs = new IndexDescriptorSequenceFile();
		String operation = args[0];

		if ("readSequenceFile".equals(operation)) {
			Text key = new Text(args[2]);
			hdfs.readSequenceFile(args[1], key);
		} else if ("writeSequenceFile".equals(operation)) {
			hdfs.writeSequenceFile(args[1], new Text(args[2]),
					hdfs.get(args[3]));
		} else {
			log.info(operation + " is not a valid operation.");
		}
	}

	public void setConfiguration() {
		this.configuration = HDFSUtil.getHDFSConfiguration();
	}

	public void setKeyClazz() {
		this.keyClass = Text.class;
	}

	public void setValueClazz() {
		this.valueClass = IndexDescriptor.class;
	}

	private IndexDescriptor get(String s) {
		String[] parts = s.split(Constant.DELIMETER_LINE);

		IndexDescriptor indexDescriptor = new IndexDescriptor();
		indexDescriptor.setDocument(new Long(parts[0]));
		indexDescriptor.setFrequency(new Integer(parts[1]));

		for (int i = 2; i < parts.length; i++) {
			String[] keyValue = parts[i].split(Constant.DELIMETER_KEY_VALUE);
			indexDescriptor.getIndex().put(new Long(keyValue[0]),
					new Long(keyValue[1]));
		}

		return indexDescriptor;
	}

}
