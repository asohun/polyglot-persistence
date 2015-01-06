package com.polyglot.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polyglot.hadoop.util.HadoopUtil;

public class TestHDFS {

	private static final Logger log = LoggerFactory.getLogger(TestHDFS.class);

	private Configuration configuration;

	public TestHDFS() {
		configuration = HadoopUtil.getHDFSConfiguration();
	}

	public static void main(String[] args) {
		TestHDFS hdfs = new TestHDFS();
		try {
			//hdfs.createFile("/test2");
			//hdfs.write("/test2", "testWrite");
			hdfs.read("/test2");
			// hdfs.createFile("/test2");
			// hdfs.writeSequenceFile("/test2", "keySequence", "valueSequence");
			// hdfs.readSequenceFile("/test2");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create a text file
	 * 
	 * @param file
	 *            the string representing the path of the file
	 * @throws IOException
	 */
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
		
		fs.close();
	}

	/**
	 * Write text content to a text file
	 * 
	 * @param file
	 *            the string representing the path of the file
	 * @param content
	 *            the string representing the content to write to the file
	 * @throws IOException
	 */
	public void write(String file, String content) throws IOException {
		FileSystem fs = FileSystem.get(configuration);
		Path path = new Path(file);
		FSDataOutputStream out = fs.append(path);
		out.write(content.getBytes());
		out.close();
		fs.close();
	}

	/**
	 * Read text file
	 * 
	 * @param file
	 *            the string representing the path of the file
	 * @return the string representing the content of the file
	 * @throws IOException
	 */
	public String read(String file) throws IOException {
		FileSystem fs = FileSystem.get(configuration);
		Path path = new Path(file);
		InputStream in = fs.open(path);

		String content = "";

		Writer writer = new StringWriter();
		org.apache.commons.io.IOUtils.copy(in, writer, "UTF-8");
		String raw = writer.toString();
		for (String str : raw.split("\n")) {
			content += str + " ";
		}

		in.close();
		fs.close();
		log.debug(content);

		return content;
	}

	/**
	 * @param file
	 *            the string representing the path of the file
	 * @param key
	 *            the string representing the key of the sequence
	 * @param value
	 *            the string representing the value of the sequence
	 * @throws IOException
	 */
	public void writeSequenceFile(String file, String key, String value)
			throws IOException {
		FileContext fileContext = FileContext.getFileContext(configuration);
		Path path = new Path(file);
		EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.APPEND);
		SequenceFile.Writer sequenceWriter = SequenceFile.createWriter(
				fileContext, configuration, path, Text.class, Text.class,
				CompressionType.NONE, null, new Metadata(), createFlag);

		Text textKey = new Text(key);
		Text textValue = new Text(value);
		sequenceWriter.append(textKey, textValue);
		IOUtils.closeStream(sequenceWriter);
	}

	/**
	 * @param file
	 *            the string representing the path of the file
	 * @throws IOException
	 */
	public void readSequenceFile(String file) throws IOException {
		Path path = new Path(file);
		SequenceFile.Reader reader = new SequenceFile.Reader(configuration,
				SequenceFile.Reader.file(path));
		Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
				configuration);
		Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(),
				configuration);
		while (reader.next(key, value)) {
			log.debug("key : " + key + " - value : "
					+ new String(value.getBytes()));
		}
		IOUtils.closeStream(reader);
	}

}