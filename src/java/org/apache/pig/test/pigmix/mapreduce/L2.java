package org.apache.pig.test.pigmix.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class L2 extends Configured implements Tool {

	/**
	 * MAPPER
	 */
	public static class Join extends Mapper<LongWritable, Text, Text, Text> {

		private Set<String> hash;

		@Override
		public void setup(Context context) {
			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());
				if (paths == null || paths.length < 1) {
					throw new RuntimeException("DistributedCache no work.");
				}

				// Open the small table
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new FileInputStream(
								paths[0].toString())));
				String line;
				hash = new HashSet<String>(500);
				while ((line = reader.readLine()) != null) {
					if (line.length() < 1)
						continue;
					String[] fields = line.split("");
					if (fields[0].length() != 0)
						hash.add(fields[0]);
				}
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}

		@Override
		public void map(LongWritable k, Text val, Context context)
				throws IOException, InterruptedException {

			List<Text> fields = Library.splitLine(val, '');

			if (hash.contains(fields.get(0).toString())) {
				context.write(fields.get(0), fields.get(6));
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

		Job job = new Job(getConf(), "PigMix L2");
		job.setJarByClass(L2.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Join.class);

		Properties props = System.getProperties();
		Configuration conf = job.getConfiguration();
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			conf.set((String) entry.getKey(), (String) entry.getValue());
		}

		DistributedCache.addCacheFile(new URI(args[0] + "/pigmix_power_users"),
				conf);
		FileInputFormat.addInputPath(job, new Path(args[0]
				+ "/pigmix_page_views"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/L2out"));
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new L2(), args);
		System.exit(res);
	}

}
