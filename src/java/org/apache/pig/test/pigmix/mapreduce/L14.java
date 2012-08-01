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

public class L14 extends Configured implements Tool {

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
			isLeft = split.getPath().toString()
					.contains("pigmix_page_views_sorted");
		}

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			if (isLeft) {
				// Process left input: page views
				List<Text> fields = Library.splitLine(val, '');
				if (fields.size() > 0 && fields.get(0).getLength() > 0) {
					OUT.set("1" + fields.get(6).toString());
					context.write(fields.get(0), OUT);
				}
			} else {
				// Process right input: power user samples
				List<Text> fields = Library.splitLine(val, '\u0001');
				if (fields.size() > 0 && fields.get(0).getLength() > 0) {
					OUT.set("2");
					context.write(fields.get(0), OUT);
				}
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
			int count = 0;

			for (Text value : values) {
				if (value.charAt(0) == '1')
					first.add(value.toString().substring(1));
				else
					++count;
				context.setStatus("OK");
			}

			context.setStatus("OK");

			if (first.size() == 0 || count == 0)
				return;

			// Do the cross product
			for (String s1 : first) {
				for (int i = 0; i < count; ++i) {
					OUT.set(key.toString() + "\t" + s1 + "\t" + key.toString());
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

		Job job = new Job(getConf(), "PigMix L14");
		job.setJarByClass(L14.class);

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
				+ "/pigmix_page_views_sorted"));
		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_users_sorted"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L14out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L14(), args);
		System.exit(res);
	}

}
