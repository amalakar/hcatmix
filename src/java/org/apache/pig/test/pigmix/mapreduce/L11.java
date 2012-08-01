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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class L11 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadInput extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private NullWritable NULL = NullWritable.get();

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line and print out the first value
			List<Text> fields = Library.splitLine(val, '');
			context.write(fields.get(0), NULL);
		}
	}

	/**
	 * REDUCER
	 */
	public static class Distinct extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		private NullWritable NULL = NullWritable.get();

		@Override
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {

			context.write(key, NULL);
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

		Job job = new Job(getConf(), "PigMix L11");
		job.setJarByClass(L11.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(ReadInput.class);
		job.setCombinerClass(Distinct.class);
		job.setReducerClass(Distinct.class);

		Properties props = System.getProperties();
		Configuration conf = job.getConfiguration();
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			conf.set((String) entry.getKey(), (String) entry.getValue());
		}

		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_page_views"));
		FileInputFormat
				.addInputPath(job, new Path(args[0] + "/pigmix_widerow"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L11out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L11(), args);
		System.exit(res);
	}

}
