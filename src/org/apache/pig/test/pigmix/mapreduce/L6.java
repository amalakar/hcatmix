package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class L6 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text OUTK = new Text();
		private IntWritable OUTV = new IntWritable();

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			StringBuilder sb = new StringBuilder();
			sb.append(fields.get(0).toString());
			sb.append("\t");
			sb.append(fields.get(3).toString());
			sb.append("\t");
			sb.append(fields.get(4).toString());
			sb.append("\t");
			sb.append(fields.get(5).toString());

			try {
				OUTK.set(sb.toString());
				OUTV.set(Integer.valueOf(fields.get(2).toString()));
				context.write(OUTK, OUTV);
			} catch (NumberFormatException nfe) {
			}
		}
	}

	/**
	 * REDUCER
	 */
	public static class Group extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable OUT = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			OUT.set(sum);
			context.write(key, OUT);
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

		Job job = new Job(getConf(), "PigMix L6");
		job.setJarByClass(L6.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(ReadPageViews.class);
		job.setCombinerClass(Group.class);
		job.setReducerClass(Group.class);

		Properties props = System.getProperties();
		Configuration conf = job.getConfiguration();
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			conf.set((String) entry.getKey(), (String) entry.getValue());
		}

		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_page_views"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L6out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L6(), args);
		System.exit(res);
	}

}
