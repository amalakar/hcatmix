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

public class L17 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class ReadPageViews extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text OUTK = new Text();
		private Text OUTV = new Text();

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			List<Text> vals = Library.splitLine(val, '');
			if (vals.size() != 27)
				return;

			StringBuilder key = new StringBuilder();
			key.append(vals.get(0).toString());
			key.append("");
			key.append(vals.get(1).toString());
			key.append("");
			key.append(vals.get(2).toString());
			key.append("");
			key.append(vals.get(3).toString());
			key.append("");
			key.append(vals.get(4).toString());
			key.append("");
			key.append(vals.get(5).toString());
			key.append("");
			key.append(vals.get(6).toString());
			key.append("");
			key.append(vals.get(9).toString());
			key.append("");
			key.append(vals.get(10).toString());
			key.append("");
			key.append(vals.get(11).toString());
			key.append("");
			key.append(vals.get(12).toString());
			key.append("");
			key.append(vals.get(13).toString());
			key.append("");
			key.append(vals.get(14).toString());
			key.append("");
			key.append(vals.get(15).toString());
			key.append("");
			key.append(vals.get(18).toString());
			key.append("");
			key.append(vals.get(19).toString());
			key.append("");
			key.append(vals.get(20).toString());
			key.append("");
			key.append(vals.get(21).toString());
			key.append("");
			key.append(vals.get(22).toString());
			key.append("");
			key.append(vals.get(23).toString());
			key.append("");
			key.append(vals.get(24).toString());

			StringBuilder sb = new StringBuilder();
			sb.append(vals.get(2).toString());
			sb.append("");
			sb.append(vals.get(11).toString());
			sb.append("");
			sb.append(vals.get(20).toString());
			sb.append("");
			sb.append(vals.get(6).toString());
			sb.append("");
			sb.append(vals.get(15).toString());
			sb.append("");
			sb.append(vals.get(24).toString());
			sb.append("");
			sb.append(1);

			OUTK.set(key.toString());
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

			int tsSum = 0, tsSum1 = 0, tsSum2 = 0, erCnt = 0;
			double erSum = 0.0, erSum1 = 0.0, erSum2 = 0.0;

			for (Text value : values) {
				List<Text> vals = Library.splitLine(value, '');
				try {
					if (vals.get(0).getLength() != 0)
						tsSum += Integer.valueOf(vals.get(0).toString());
					if (vals.get(1).getLength() != 0)
						tsSum1 += Integer.valueOf(vals.get(1).toString());
					if (vals.get(2).getLength() != 0)
						tsSum2 += Integer.valueOf(vals.get(2).toString());
					if (vals.get(3).getLength() != 0)
						erSum += Double.valueOf(vals.get(3).toString());
					if (vals.get(4).getLength() != 0)
						erSum1 += Double.valueOf(vals.get(4).toString());
					if (vals.get(5).getLength() != 0)
						erSum2 += Double.valueOf(vals.get(5).toString());
					if (vals.get(6).getLength() != 0)
						erCnt += Integer.valueOf(vals.get(6).toString());
				} catch (NumberFormatException nfe) {
					System.err.println(nfe.getMessage());
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append(tsSum);
			sb.append("");
			sb.append(tsSum1);
			sb.append("");
			sb.append(tsSum2);
			sb.append("");
			sb.append(erSum);
			sb.append("");
			sb.append(erSum1);
			sb.append("");
			sb.append(erSum2);
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

			int tsSum = 0, tsSum1 = 0, tsSum2 = 0, erCnt = 0;
			double erSum = 0.0, erSum1 = 0.0, erSum2 = 0.0;

			for (Text value : values) {
				List<Text> vals = Library.splitLine(value, '');
				try {
					if (vals.get(0).getLength() != 0)
						tsSum += Integer.valueOf(vals.get(0).toString());
					if (vals.get(1).getLength() != 0)
						tsSum1 += Integer.valueOf(vals.get(1).toString());
					if (vals.get(2).getLength() != 0)
						tsSum2 += Integer.valueOf(vals.get(2).toString());
					if (vals.get(3).getLength() != 0)
						erSum += Double.valueOf(vals.get(3).toString());
					if (vals.get(4).getLength() != 0)
						erSum1 += Double.valueOf(vals.get(4).toString());
					if (vals.get(5).getLength() != 0)
						erSum2 += Double.valueOf(vals.get(5).toString());
					if (vals.get(6).getLength() != 0)
						erCnt += Integer.valueOf(vals.get(6).toString());
				} catch (NumberFormatException nfe) {
					System.err.println(nfe.getMessage());
				}
			}
			double erAvg = erSum / erCnt;
			double erAvg1 = erSum1 / erCnt;
			double erAvg2 = erSum2 / erCnt;

			StringBuilder sb = new StringBuilder();
			sb.append(tsSum);
			sb.append("\t");
			sb.append(tsSum1);
			sb.append("\t");
			sb.append(tsSum2);
			sb.append("\t");
			sb.append(erAvg);
			sb.append("\t");
			sb.append(erAvg1);
			sb.append("\t");
			sb.append(erAvg2);

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

		Job job = new Job(getConf(), "PigMix L17");
		job.setJarByClass(L17.class);

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
				+ "/pigmix_widegroupbydata"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L17out"));
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L17(), args);
		System.exit(res);
	}

}
