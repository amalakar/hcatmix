package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class L9 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			context.write(fields.get(3), val);
		}
	}

	/**
	 * PARTITIONER
	 */
	public static class MyPartitioner extends Partitioner<Text, Text> {

		private int nextEmpty = 0;

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {

			if (numPartitions == 1)
				return 0;

			int emptyParts = numPartitions / 5;
			if (emptyParts == 0)
				emptyParts = 1;
			int otherParts = numPartitions - emptyParts;

			if (key.getLength() == 0 || key.charAt(0) < 'A') {
				nextEmpty = ++nextEmpty % emptyParts;
				return nextEmpty;
			}

			double interval = 57.0 / otherParts;
			int part = (int) ((key.charAt(0) - 'A') / interval) + emptyParts;
			if (part >= numPartitions)
				return numPartitions - 1;
			else
				return part;
		}

	}

	/**
	 * REDUCER
	 */
	public static class Group extends Reducer<Text, Text, NullWritable, Text> {

		private NullWritable NULL = NullWritable.get();
		private Text OUTV = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				OUTV.set(value.toString().replace('', '\t'));
				context.write(NULL, OUTV);
			}
		}
	}

	/**
	 * RUN
	 */
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.err
					.println("Usage: wordcount <input_dir> <output_dir> <reducers>");
			return -1;
		}

		Job job = new Job(getConf(), "PigMix L9");
		job.setJarByClass(L9.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ReadPageViews.class);
		job.setReducerClass(Group.class);
		job.setPartitionerClass(MyPartitioner.class);

		Properties props = System.getProperties();
		Configuration conf = job.getConfiguration();
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			conf.set((String) entry.getKey(), (String) entry.getValue());
		}

		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_page_views"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L9out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L9(), args);
		System.exit(res);
	}

}
