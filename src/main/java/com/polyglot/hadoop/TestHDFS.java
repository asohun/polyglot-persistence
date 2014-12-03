package com.polyglot.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Writable;

public class TestHDFS {

	public void test() throws IOException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);

		Path filePath = new Path("");
		if (fs.exists(filePath)) {

		}

		if (fs.isFile(filePath)) {

		}

		Boolean result = fs.createNewFile(filePath);
		result = fs.delete(filePath);

		FSDataInputStream in = fs.open(filePath);
		FSDataOutputStream out = fs.create(filePath);
	}

	public void sequenceFile() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("fileName");
		SequenceFile.Writer sequenceWriter = new SequenceFile.Writer(fs, conf,
				path, String.class, String.class, fs.getConf().getInt(
						"io.file.buffer.size", 4096),
				fs.getDefaultReplication(), 1073741824, null, new Metadata());
		Writable bytesWritable = null;
		sequenceWriter.append(bytesWritable, bytesWritable);
		IOUtils.closeStream(sequenceWriter);
	}
}
