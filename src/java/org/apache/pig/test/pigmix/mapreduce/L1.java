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

public class L1 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private IntWritable COUNT = new IntWritable();

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			int cnt = 0;
			if (fields.get(1).toString() == "1") {
				Library.mapLookup(fields.get(7), new Text("a"));
				cnt++;
			} else {
				List<Text> le = Library.splitLine(fields.get(8), '');
				for (Text e : le) {
					Library.mapLookup(e, new Text("b"));
					cnt++;
				}
			}

			COUNT.set(cnt);
			context.write(fields.get(0), COUNT);
		}
	}

	/**
	 * REDUCER
	 */
	public static class Group extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable COUNT = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int cnt = 0;
			for (IntWritable val : values) {
				cnt += val.get();
			}

			COUNT.set(cnt);
			context.write(key, COUNT);
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

		Job job = new Job(getConf(), "PigMix L1");
		job.setJarByClass(L1.class);

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
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L1out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L1(), args);
		System.exit(res);
	}

}
