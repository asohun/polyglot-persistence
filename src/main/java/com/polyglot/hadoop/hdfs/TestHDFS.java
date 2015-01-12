package com.polyglot.hadoop.hdfs;

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
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polyglot.hadoop.util.HDFSUtil;

/**
 * @author asohun
 */
public class TestHDFS {

	private static final Logger log = LoggerFactory.getLogger(TestHDFS.class);

	private Configuration configuration;

	public TestHDFS() {
		configuration = HDFSUtil.getHDFSConfiguration();
	}

	public static void main(String[] args) {
		TestHDFS hdfs = new TestHDFS();
		String operation = args[0];

		if ("create".equals(operation)) {
			hdfs.create(args[1]);
		} else if ("delete".equals(operation)) {
			hdfs.delete(args[1]);
		} else if ("read".equals(operation)) {
			hdfs.read(args[1]);
		} else if ("write".equals(operation)) {
			hdfs.write(args[1], args[2]);
		} else if ("readSequenceFile".equals(operation)) {
			hdfs.readSequenceFile(args[1]);
		} else if ("writeSequenceFile".equals(operation)) {
			hdfs.writeSequenceFile(args[1], args[2], args[3]);
		} else {
			log.info(operation + " is not a valid operation.");
		}
	}

	/**
	 * Create a text file
	 * 
	 * @param file
	 *            the string representing the path of the file
	 */
	public void create(String file) {
		FileSystem fs = null;

		Path path = new Path(file);
		boolean created = false;

		try {
			fs = FileSystem.get(configuration);

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
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	/**
	 * Delete file
	 * 
	 * @param file
	 *            the string representing the path of the file
	 */
	public void delete(String file) {
		FileSystem fs = null;

		Path path = new Path(file);

		try {
			fs = FileSystem.get(configuration);
			fs.delete(path, true);
			log.debug(file + " deleted.");
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	/**
	 * Write text content to a text file
	 * 
	 * @param file
	 *            the string representing the path of the file
	 * @param content
	 *            the string representing the content to write to the file
	 */
	public void write(String file, String content) {
		FileSystem fs = null;
		FSDataOutputStream out = null;

		Path path = new Path(file);

		try {
			fs = FileSystem.get(configuration);
			out = fs.append(path);
			out.write(content.getBytes());
			log.debug(content + " written to " + file);
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}

			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}

	}

	/**
	 * Read text file
	 * 
	 * @param file
	 *            the string representing the path of the file
	 * @return the string representing the content of the file
	 */
	public String read(String file) {
		FileSystem fs = null;
		InputStream in = null;

		StringBuffer content = new StringBuffer();
		Path path = new Path(file);
		Writer writer = new StringWriter();

		try {
			fs = FileSystem.get(configuration);
			in = fs.open(path);

			org.apache.commons.io.IOUtils.copy(in, writer, "UTF-8");
			String raw = writer.toString();
			for (String str : raw.split("\n")) {
				content.append(str + " ");
			}

			in.close();
			fs.close();
			log.debug(content.toString());
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}

			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}

		return content.toString();
	}

	/**
	 * @param file
	 *            the string representing the path of the file
	 * @param key
	 *            the string representing the key of the sequence
	 * @param value
	 *            the string representing the value of the sequence
	 */
	public void writeSequenceFile(String file, String key, String value) {
		FileContext fileContext = null;
		SequenceFile.Writer sequenceWriter = null;

		Path path = new Path(file);
		EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.APPEND);
		Text textKey = new Text(key);
		Text textValue = new Text(value);

		try {
			fileContext = FileContext.getFileContext(configuration);
			sequenceWriter = SequenceFile.createWriter(fileContext,
					configuration, path, Text.class, Text.class,
					CompressionType.NONE, null, new Metadata(), createFlag);
			sequenceWriter.append(textKey, textValue);
		} catch (UnsupportedFileSystemException e) {
			log.error(e.getMessage());
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (sequenceWriter != null) {
				IOUtils.closeStream(sequenceWriter);
			}
		}
	}

	/**
	 * @param file
	 *            the string representing the path of the file
	 * @throws IOException
	 */
	public void readSequenceFile(String file) {
		Path path = new Path(file);
		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(configuration,
					SequenceFile.Reader.file(path));
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
					configuration);
			Text value = (Text) ReflectionUtils.newInstance(
					reader.getValueClass(), configuration);
			while (reader.next(key, value)) {
				log.debug("key : " + key + " - value : "
						+ new String(value.getBytes()));
			}
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (reader != null) {
				IOUtils.closeStream(reader);
			}
		}
	}

}