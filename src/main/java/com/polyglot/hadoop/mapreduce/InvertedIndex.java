package com.polyglot.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashMap;
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

public class InvertedIndex extends Configured implements Tool {

	private static final Logger log = LoggerFactory
			.getLogger(InvertedIndex.class);

	private Configuration configuration;

	public InvertedIndex() {
		configuration = HDFSUtil.getHDFSConfiguration();
	}

	// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	public static class Map extends
			Mapper<LongWritable, Text, Text, IndexDescriptor> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			log.debug("Map executed.");
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				IndexDescriptor indexDescriptor = new IndexDescriptor();
				indexDescriptor.setDocument(key.get());
				indexDescriptor.setFrequency(1);
				indexDescriptor.getIndex().put(key.get(), 1l);

				Text keyOut = new Text(tokenizer.nextToken());
				context.write(keyOut, indexDescriptor);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IndexDescriptor, Text, IndexDescriptor> {
		@Override
		public void reduce(Text key, Iterable<IndexDescriptor> val,
				Context context) throws IOException, InterruptedException {
			log.debug("Reduce executed.");
			int count = 0;
			java.util.Map<Long, Long> index = new HashMap<Long, Long>();
			IndexDescriptor valueOut = new IndexDescriptor();
			Iterator<IndexDescriptor> values = val.iterator();

			while (values.hasNext()) {
				IndexDescriptor descriptor = values.next();
				count += descriptor.getFrequency();
				index.put(descriptor.getIndex().get(0), 1l);
			}
			valueOut.setFrequency(count);
			valueOut.setIndex(index);
			context.write(key, valueOut);
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(configuration);
		job.setJarByClass(InvertedIndex.class);

		// Set up the input
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		// job.setInputFormatClass(SequenceFileInputFormat.class);
		// SequenceFileInputFormat.addInputPath(job, new Path(args[0]));

		job.setMapperClass(Map.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IndexDescriptor.class);

		job.setReducerClass(Reduce.class);

		// Output
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IndexDescriptor.class);
		// SequenceFileOutputFormat.setOutputPath(job,new Path(args[1] +
		// "/tmp"));

		// Execute
		boolean res = job.waitForCompletion(true);
		if (res) {
			return 0;
		} else {
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WordCount(), args);
		System.exit(res);
	}

}
