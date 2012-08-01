package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class L15 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text OUTV = new Text();

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			StringBuilder sb = new StringBuilder();
			sb.append(fields.get(1).toString()); // action
			sb.append('');
			sb.append(fields.get(6).toString()); // estimated_revenue
			sb.append('');
			sb.append(fields.get(2).toString()); // timespent
			sb.append('');

			OUTV.set(sb.toString());
			context.write(fields.get(0), OUTV);
		}
	}

	/**
	 * COMBINER
	 */
	public static class Combiner extends Reducer<Text, Text, Text, Text> {

		private Text OUTV = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashSet<Text> hash1 = new HashSet<Text>();
			HashSet<Text> hash2 = new HashSet<Text>();
			HashSet<Text> hash3 = new HashSet<Text>();

			for (Text value : values) {
				List<Text> vals = Library.splitLine(value, '');
				try {
					hash1.add(vals.get(0));
					hash2.add(vals.get(1));
					hash3.add(vals.get(2));
				} catch (NumberFormatException nfe) {
				}
			}

			Double rev = new Double(0.0);
			Integer ts = 0;
			try {
				for (Text t : hash2)
					rev += Double.valueOf(t.toString());
				for (Text t : hash3)
					ts += Integer.valueOf(t.toString());
			} catch (NumberFormatException e) {
			}

			StringBuilder sb = new StringBuilder();
			sb.append(hash1.size());
			sb.append("");
			sb.append(rev.toString());
			sb.append("");
			sb.append(ts.toString());
			sb.append("");

			OUTV.set(sb.toString());
			context.write(key, OUTV);
			context.setStatus("OK");
		}
	}

	/**
	 * REDUCER
	 */
	public static class Group extends Reducer<Text, Text, Text, Text> {

		private Text OUTV = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashSet<Text> hash1 = new HashSet<Text>();
			HashSet<Text> hash2 = new HashSet<Text>();
			HashSet<Text> hash3 = new HashSet<Text>();

			for (Text value : values) {
				List<Text> vals = Library.splitLine(value, '');
				try {
					hash1.add(vals.get(0));
					hash2.add(vals.get(1));
					hash3.add(vals.get(2));
				} catch (NumberFormatException nfe) {
				}
			}

			Integer ts = 0;
			Double rev = new Double(0.0);
			try {
				for (Text t : hash2)
					rev += Double.valueOf(t.toString());
				for (Text t : hash3)
					ts += Integer.valueOf(t.toString());
			} catch (NumberFormatException nfe) {
			}

			StringBuilder sb = new StringBuilder();
			sb.append(hash1.size());
			sb.append("");
			sb.append(rev.toString());
			sb.append("");
			sb.append(ts.toString());

			OUTV.set(sb.toString());
			context.write(key, OUTV);
			context.setStatus("OK");
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

		Job job = new Job(getConf(), "PigMix L15");
		job.setJarByClass(L15.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ReadPageViews.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Group.class);

		Properties props = System.getProperties();
		Configuration conf = job.getConfiguration();
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			conf.set((String) entry.getKey(), (String) entry.getValue());
		}

		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_page_views"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L15out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L15(), args);
		System.exit(res);
	}

}
