package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.test.output.MultiTextOutputFormat;
import org.apache.pig.test.output.MultiTextRecordWriterSelector;

public class L12 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text OUTK = new Text();
		private DoubleWritable OUTV = new DoubleWritable();

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			// Split the line
			List<Text> fields = Library.splitLine(val, '');
			if (fields.size() != 9)
				return;

			// Filter out null users and query terms. Group by user
			if (fields.get(0).getLength() != 0
					&& fields.get(3).getLength() != 0) {
				OUTK.set("1" + fields.get(0).toString());
				try {
					OUTV.set(Double.valueOf(fields.get(6).toString()));
				} catch (NumberFormatException nfe) {
					OUTV.set(0);
				}
				context.write(OUTK, OUTV);
			}

			// Filter out non-null users. Group by query_term
			if (fields.get(0).getLength() == 0) {
				OUTK.set("2" + fields.get(3).toString());
				try {
					OUTV.set(Double.valueOf(fields.get(2).toString()));
				} catch (NumberFormatException nfe) {
					OUTV.set(0);
				}
				context.write(OUTK, OUTV);
			}

			// Filter out null users and non-null queries. Group by action
			if (fields.get(0).getLength() != 0
					&& fields.get(3).getLength() == 0) {
				OUTK.set("3" + fields.get(1).toString());
				OUTV.set(1);
				context.write(OUTK, OUTV);
			}
		}
	}

	/**
	 * COMBINER
	 */
	public static class Group extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable OUTV = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			switch (key.charAt(0)) {
			case '1': {
				double max = Double.MIN_VALUE;
				for (DoubleWritable value : values)
					if (value.get() > max)
						max = value.get();
				OUTV.set(max);
				context.write(key, OUTV);
				break;
			}
			case '2': {
				double sum = 0;
				for (DoubleWritable value : values)
					sum += value.get();
				OUTV.set(sum);
				context.write(key, OUTV);
				break;
			}
			case '3': {
				long cnt = 0;
				for (DoubleWritable value : values)
					cnt += value.get();
				OUTV.set(cnt);
				context.write(key, OUTV);
				break;
			}
			}
		}
	}

	/**
	 * A record writer selector the knows the tag is appended to the key
	 * 
	 * @author hero
	 */
	static class RecordWriterSelectorOnKey extends
			MultiTextRecordWriterSelector<Text, DoubleWritable> {

		public RecordWriterSelectorOnKey() {
			super();
		}

		@Override
		protected int getWriterIndex(Text key, DoubleWritable value) {
			String keyStr = key.toString();
			return Character.getNumericValue(keyStr.charAt(0)) - 1;
		}

		@Override
		protected Text generateActualKey(Text key, DoubleWritable value) {
			String keyStr = key.toString();
			key.set(keyStr.substring(1));
			return key;
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

		Job job = new Job(getConf(), "PigMix L12");
		job.setJarByClass(L12.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
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
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L12out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		// Setup the multi-output format
		MultiTextOutputFormat.addOutputPath(job, new Path(args[1]
				+ "/L12out/highest_value_page_per_user"));
		MultiTextOutputFormat.addOutputPath(job, new Path(args[1]
				+ "/L12out/total_timespent_per_term"));
		MultiTextOutputFormat.addOutputPath(job, new Path(args[1]
				+ "/L12out/queries_per_action"));
		MultiTextOutputFormat.setRecordWriterSelectorClass(job,
				RecordWriterSelectorOnKey.class);
		job.setOutputFormatClass(MultiTextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L12(), args);
		System.exit(res);
	}

}
