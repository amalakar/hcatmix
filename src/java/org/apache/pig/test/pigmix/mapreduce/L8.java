package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
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

public class L8 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text OUTK = new Text("all");
		private Text OUTV = new Text();

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			StringBuilder sb = new StringBuilder();
			sb.append(fields.get(2).toString());
			sb.append("\t");
			sb.append(fields.get(6).toString());
			sb.append("\t");
			sb.append("1");

			OUTV.set(sb.toString());
			context.write(OUTK, OUTV);
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

			int tsSum = 0, erCnt = 0;
			double erSum = 0.0;

			for (Text value : values) {
				List<Text> vals = Library.splitLine(value, '\t');
				try {
					System.out.println(value);
					System.out.println(vals);
					tsSum += Integer.valueOf(vals.get(0).toString());
					erSum += Double.valueOf(vals.get(1).toString());
					erCnt += Integer.valueOf(vals.get(2).toString());
				} catch (NumberFormatException nfe) {
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append(tsSum);
			sb.append("");
			sb.append(erSum);
			sb.append("");
			sb.append(erCnt);

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

			int tsSum = 0, erCnt = 0;
			double erSum = 0.0;

			for (Text value : values) {
				List<Text> vals = Library.splitLine(value, '');
				try {
					tsSum += Integer.valueOf(vals.get(0).toString());
					erSum += Double.valueOf(vals.get(1).toString());
					erCnt += Integer.valueOf(vals.get(2).toString());
				} catch (NumberFormatException nfe) {
				}
			}

			double erAvg = erSum / erCnt;
			StringBuilder sb = new StringBuilder();
			sb.append(tsSum);
			sb.append("\t");
			sb.append(erAvg);

			OUTV.set(sb.toString());
			context.write(null, OUTV);
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

		Job job = new Job(getConf(), "PigMix L8");
		job.setJarByClass(L8.class);

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
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L8out"));
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L8(), args);
		System.exit(res);
	}

}
