package com.polyglot.hadoop;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.polyglot.hadoop.util.HadoopUtil;

public class TestHDFS {

	private static final Logger log = LogManager.getLogger(TestHDFS.class);

	private Configuration configuration;

	public TestHDFS() {
		configuration = HadoopUtil.getHDFSConfiguration();
	}

	public static void main(String[] args) {
		TestHDFS hdfs = new TestHDFS();

		try {
			hdfs.createFile("/test1");
			hdfs.writeSequenceFile("/test1");
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
		FileContext fileContext = FileContext.getFileContext(configuration);
		Path path = new Path(file);
		EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.APPEND);
		SequenceFile.Writer sequenceWriter = SequenceFile.createWriter(
				fileContext, configuration, path, Text.class, Text.class,
				CompressionType.NONE, null, new Metadata(), createFlag);

		Text key = new Text("keySequence");
		Text value = new Text("valueSequence");
		sequenceWriter.append(key, value);
		IOUtils.closeStream(sequenceWriter);
	}

}