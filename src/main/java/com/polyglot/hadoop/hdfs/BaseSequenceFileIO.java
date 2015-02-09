package com.polyglot.hadoop.hdfs;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSequenceFileIO<K extends Writable, V extends Writable>
		implements SequenceFileIO {

	private final Logger log = LoggerFactory
			.getLogger(BaseSequenceFileIO.class);

	protected Configuration configuration;
	protected Class<? extends Writable> keyClass;
	protected Class<? extends Writable> valueClass;

	/**
	 * @param file
	 *            the string representing the path of the file
	 * @param key
	 *            the object representing the key of the sequence
	 * @param value
	 *            the object representing the value of the sequence
	 */
	public void writeSequenceFile(String file, K key, V value) {
		FileContext fileContext = null;
		SequenceFile.Writer sequenceWriter = null;

		Path path = new Path(file);
		EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.APPEND);

		try {
			CompressionCodec compressionCodec = null;
			fileContext = FileContext.getFileContext(configuration);
			sequenceWriter = SequenceFile.createWriter(fileContext,
					configuration, path, keyClass, valueClass,
					CompressionType.NONE, compressionCodec, new Metadata(),
					createFlag);
			sequenceWriter.append(key, value);
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
	 * @param key
	 *            the object representing the key of the sequenceFile to
	 *            retrieve
	 * @throws IOException
	 */
	public V readSequenceFile(String file, K key) {
		Path path = new Path(file);
		SequenceFile.Reader reader = null;
		V value = null;

		try {
			reader = new SequenceFile.Reader(configuration,
					SequenceFile.Reader.file(path));
			while (reader.next(key, value)) {
				log.debug("key : " + key + " - value : " + value);
			}
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (reader != null) {
				IOUtils.closeStream(reader);
			}
		}

		return value;
	}

}
