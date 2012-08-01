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

public class L7 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			context.write(fields.get(0), fields.get(5));
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

			int morning = 0, afternoon = 0;
			for (Text value : values) {
				try {
					if (Integer.valueOf(value.toString()) < 43200)
						morning++;
					else
						afternoon++;
				} catch (NumberFormatException nfe) {
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append((new Integer(morning)).toString());
			sb.append("\t");
			sb.append((new Integer(afternoon)).toString());

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

			int morning = 0, afternoon = 0;
			for (Text value : values) {
				List<Text> vals = Library.splitLine(value, '');
				try {
					morning += Integer.valueOf(vals.get(0).toString());
					if (vals.size() > 1)
						afternoon += Integer.valueOf(vals.get(1).toString());
				} catch (NumberFormatException nfe) {
				}
			}
			StringBuilder sb = new StringBuilder();
			sb.append((new Integer(morning)).toString());
			sb.append("\t");
			sb.append((new Integer(afternoon)).toString());

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

		Job job = new Job(getConf(), "PigMix L7");
		job.setJarByClass(L7.class);

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
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L7out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L7(), args);
		System.exit(res);
	}

}
