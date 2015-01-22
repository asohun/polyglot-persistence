package com.polyglot.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polyglot.hadoop.util.HDFSUtil;

/**
 * @author anoop
 */
public class WordCount extends Configured implements Tool {

	private static final Logger log = LoggerFactory.getLogger(WordCount.class);

	private Configuration configuration;

	/**
	 * Default constructor.
	 */
	public WordCount() {
		configuration = HDFSUtil.getHDFSConfiguration();
	}

	/**
	 * Map function.
	 * 
	 * @author anoop
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			log.debug("Executing map function.");
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				word.set(token);
				context.write(word, one);
			}
		}
	}

	/**
	 * Reduce function.
	 * 
	 * @author anoop
	 */
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> val, Context context)
				throws IOException, InterruptedException {
			log.debug("Executing reduce function.");
			int sum = 0;
			Iterator<IntWritable> values = val.iterator();
			while (values.hasNext()) {
				sum += values.next().get();
			}
			log.debug(key.toString() + " - " + sum);
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * 
	 */
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(configuration);
		job.setJarByClass(WordCount.class);

		// Set up the input
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// Output
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		// Execute
		boolean res = job.waitForCompletion(true);
		if (res) {
			return 0;
		} else {
			return -1;
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WordCount(), args);
		System.exit(res);
	}

}
