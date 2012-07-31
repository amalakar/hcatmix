package org.apache.pig.test.pigmix.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class L10 extends Configured implements Tool {

	/**
	 * CUSTOM WRITABLE
	 */
	public static class MyType implements WritableComparable<MyType> {

		public String query_term;
		double estimated_revenue;
		int timespent;

		public MyType() {
			query_term = null;
			timespent = 0;
			estimated_revenue = 0.0;
		}

		public MyType(Text qt, Text ts, Text er) {
			query_term = qt.toString();
			try {
				timespent = Integer.valueOf(ts.toString());
			} catch (NumberFormatException nfe) {
				timespent = 0;
			}
			try {
				estimated_revenue = Double.valueOf(er.toString());
			} catch (NumberFormatException nfe) {
				estimated_revenue = 0.0;
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(timespent);
			out.writeDouble(estimated_revenue);
			out.writeInt(query_term.length());
			out.writeBytes(query_term);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			timespent = in.readInt();
			estimated_revenue = in.readDouble();
			int len = in.readInt();
			byte[] b = new byte[len];
			in.readFully(b);
			query_term = new String(b);
		}

		@Override
		public int compareTo(MyType other) {
			int rc = query_term.compareTo(other.query_term);
			if (rc != 0)
				return rc;
			if (estimated_revenue < other.estimated_revenue)
				return 1;
			else if (estimated_revenue > other.estimated_revenue)
				return -1;
			if (timespent < other.timespent)
				return -1;
			else if (timespent > other.timespent)
				return 1;
			return 0;
		}
	}

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, MyType, Text> {

		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			context.write(
					new MyType(fields.get(3), fields.get(2), fields.get(6)),
					val);
		}
	}

	/**
	 * PARTITIONER
	 */
	public static class MyPartitioner extends Partitioner<MyType, Text> {

		private int nextEmpty = 0;

		@Override
		public int getPartition(MyType key, Text value, int numPartitions) {

			if (numPartitions == 1)
				return 0;

			int emptyParts = numPartitions / 5;
			if (emptyParts == 0)
				emptyParts = 1;
			int otherParts = numPartitions - emptyParts;

			if (key.query_term.length() == 0 || key.query_term.charAt(0) < 'A') {
				nextEmpty = ++nextEmpty % emptyParts;
				return nextEmpty;
			}

			double interval = 57.0 / otherParts;
			int part = (int) ((key.query_term.charAt(0) - 'A') / interval)
					+ emptyParts;
			if (part >= numPartitions)
				return numPartitions - 1;
			else
				return part;
		}

	}

	/**
	 * REDUCER
	 */
	public static class Group extends Reducer<MyType, Text, NullWritable, Text> {

		private NullWritable NULL = NullWritable.get();
		private Text OUTV = new Text();

		@Override
		public void reduce(MyType key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				OUTV.set(value.toString().replace('', '\t'));
				context.write(NULL, value);
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

		Job job = new Job(getConf(), "PigMix L10");
		job.setJarByClass(L10.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(MyType.class);
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
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L10out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L10(), args);
		System.exit(res);
	}

}
