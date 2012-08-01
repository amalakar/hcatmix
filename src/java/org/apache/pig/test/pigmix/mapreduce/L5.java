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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class L5 extends Configured implements Tool {

	/**
	 * MAPPER 1
	 */
	public static class ReadInput extends
			Mapper<LongWritable, Text, Text, Text> {

		private boolean first = false;
		private Text ONE = new Text("1");
		private Text TWO = new Text("2");

		@Override
		public void setup(Context context) {
			FileSplit split = (FileSplit) context.getInputSplit();
			first = split.getPath().toString().contains("pigmix_page_views");
		}

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			List<Text> fields = Library.splitLine(val, '');

			// Prepend an index to the value so we know which file
			// it came from.
			if (first)
				context.write(fields.get(0), ONE);
			else
				context.write(fields.get(0), TWO);
		}
	}

	/**
	 * REDUCER
	 */
	public static class Join extends Reducer<Text, Text, NullWritable, Text> {

		private NullWritable NULL = NullWritable.get();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// For each value, figure out which file it's from and store it
			// accordingly.
			int cnt = 0;

			for (Text value : values) {
				if (value.toString().charAt(0) == '2')
					cnt++;
			}

			if (cnt == 0)
				context.write(NULL, key);
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

		Job job = new Job(getConf(), "PigMix L5");
		job.setJarByClass(L5.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ReadInput.class);
		job.setReducerClass(Join.class);

		Properties props = System.getProperties();
		Configuration conf = job.getConfiguration();
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			conf.set((String) entry.getKey(), (String) entry.getValue());
		}

		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_page_views"));
		FileInputFormat.addInputPath(job, new Path(args[0] + "/pigmix_users"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L5out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L5(), args);
		System.exit(res);
	}

}
