package com.polyglot.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.polyglot.hadoop.util.HadoopUtil;

public class TestHDFS {

	private static final Logger log = Logger.getLogger(TestHDFS.class);

	private Configuration configuration;

	public TestHDFS() {
		configuration = HadoopUtil.getHDFSConfiguration();
	}

	public static void main(String[] args) {
		TestHDFS hdfs = new TestHDFS();
		
		try {
			hdfs.createFile("/test1");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createFile(String file) throws IOException {
		FileSystem fs = FileSystem.get(configuration);

		Path path = new Path(file);
		boolean created = false;

		if (fs.exists(path)) {
			log.debug(file + " exists");
		}

		if (fs.isFile(path)) {
			log.debug(file + " is a file");
		}

		created = fs.createNewFile(path);

		if (created) {
			log.debug(path + " created");
		}

		// FSDataInputStream in = fs.open(filePath);
		// FSDataOutputStream out = fs.create(filePath);
	}

	public void writeSequenceFile(String file) throws IOException {
		FileSystem fs = FileSystem.get(configuration);
		Path path = new Path(file);
		SequenceFile.Writer sequenceWriter = new SequenceFile.Writer(fs,
				configuration, path, String.class, String.class, fs.getConf()
						.getInt("io.file.buffer.size", 4096),
				fs.getDefaultReplication(path), 1073741824, null,
				new Metadata());
		Writable bytesWritable = null;
		sequenceWriter.append(bytesWritable, bytesWritable);
		IOUtils.closeStream(sequenceWriter);
	}

}