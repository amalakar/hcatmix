package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
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

public class L13 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadInput extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text OUT = new Text();
		private boolean isLeft = false;

		@Override
		public void setup(Context context) {
			FileSplit split = (FileSplit) context.getInputSplit();
			isLeft = split.getPath().toString().contains("pigmix_page_views");
		}

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			if (isLeft) {
				// Process left input: page views
				List<Text> fields = Library.splitLine(val, '');
				OUT.set("1" + fields.get(6).toString());
				context.write(fields.get(0), OUT);

			} else {
				// Process right input: power user samples
				List<Text> fields = Library.splitLine(val, '\u0001');
				if (fields.size() == 0)
					return;
				OUT.set("2" + fields.get(1).toString());
				context.write(fields.get(0), OUT);
			}
		}
	}

	/**
	 * REDUCER
	 */
	public static class Join extends Reducer<Text, Text, NullWritable, Text> {

		private NullWritable NULL = NullWritable.get();
		private Text OUT = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// For each value, figure out which file it's from and store it
			// accordingly.
			List<String> first = new ArrayList<String>();
			List<String> second = new ArrayList<String>();

			for (Text value : values) {
				if (value.charAt(0) == '1') {
					first.add(value.toString().substring(1));
				} else
					second.add(value.toString().substring(1));
				context.setStatus("OK");
			}

			context.setStatus("OK");

			if (first.size() == 0)
				return;
			if (second.size() == 0)
				second.add(null);

			// Do the cross product
			for (String s1 : first) {
				for (String s2 : second) {
					if (s2 == null)
						OUT.set(key.toString() + "\t" + s1 + "\t\t");
					else
						OUT.set(key.toString() + "\t" + s1 + "\t"
								+ key.toString() + "\t" + s2);
					context.write(NULL, OUT);
				}
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

		Job job = new Job(getConf(), "PigMix L13");
		job.setJarByClass(L13.class);

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
		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_power_users_samples"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L13out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L13(), args);
		System.exit(res);
	}

}
