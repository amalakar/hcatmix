package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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

public class L3 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadInput extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text OUT = new Text();
		private boolean first = false;

		@Override
		public void setup(Context context) {
			FileSplit split = (FileSplit) context.getInputSplit();
			first = split.getPath().toString().contains("pigmix_page_views");
		}

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			List<Text> fields = Library.splitLine(val, '');

			if (fields.get(0).getLength() > 0) {
				// Prepend an index to the value so we know which file
				// it came from.
				if (first)
					OUT.set("1" + fields.get(6).toString());
				else
					OUT.set("2");
				context.write(fields.get(0), OUT);
			}
		}
	}

	/**
	 * REDUCER
	 */
	public static class Join extends Reducer<Text, Text, Text, DoubleWritable> {

		private DoubleWritable OUTV = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// For each value, figure out which file it's from and store it
			// accordingly.
			List<String> first = new ArrayList<String>();
			int second = 0;

			for (Text t : values) {
				String value = t.toString();
				if (value.charAt(0) == '1')
					first.add(value.substring(1));
				else
					++second;
				context.setStatus("OK");
			}

			context.setStatus("OK");

			if (first.size() == 0 || second == 0)
				return;

			// Do the cross product, and calculate the sum
			double sum = 0.0;
			for (String s1 : first) {
				try {
					sum += second * Double.valueOf(s1);
				} catch (NumberFormatException nfe) {
				}
			}

			OUTV.set(sum);
			context.write(key, OUTV);
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

		Job job = new Job(getConf(), "PigMix L3");
		job.setJarByClass(L3.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
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
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L3out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L3(), args);
		System.exit(res);
	}

}
