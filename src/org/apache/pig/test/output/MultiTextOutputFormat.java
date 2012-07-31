package org.apache.pig.test.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Used to create multiple output files. The selection of the output file is
 * based on a user-provided RecordWriterSelector
 * 
 * @author hero
 */
public class MultiTextOutputFormat<K, V> extends OutputFormat<K, V> {

	private static final String OUTPUT_DIR = "multitext.output.dir";
	private static final String RECORD_WRITE_SELECTOR = "multitext.record.writer.selector";
	private static final String MAPRED_SEPARATOR = "mapred.textoutputformat.separator";

	private MultiTextOutputCommitter committer = null;

	@Override
	public void checkOutputSpecs(JobContext job) throws IOException,
			InterruptedException {
		// Ensure that the output directory is set and not already there
		Path[] outDirs = getOutputPaths(job);

		if (outDirs == null || outDirs.length == 0) {
			throw new InvalidJobConfException("Output directory not set.");
		}

		for (Path outDir : outDirs) {
			if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
				throw new FileAlreadyExistsException("Output directory "
						+ outDir + " already exists");
			}
		}
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		if (committer == null) {
			Path[] outputPaths = getOutputPaths(context);
			committer = new MultiTextOutputCommitter(outputPaths, context);
		}
		return committer;
	}

	@SuppressWarnings("unchecked")
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String keyValueSeparator = conf.get(MAPRED_SEPARATOR, "\t");
		String extension = "";

		// Create the output streams
		Path files[] = getDefaultWorkFiles(context, extension);
		DataOutputStream[] outStreams = null;

		if (files != null && files.length != 0) {
			outStreams = new DataOutputStream[files.length];
			for (int i = 0; i < files.length; ++i) {
				outStreams[i] = files[i].getFileSystem(conf).create(files[i],
						false);
			}
		}

		// Create the record writer selector
		Class<? extends MultiTextRecordWriterSelector> selectorClass = getRecordWriterSelectorClass(context);
		MultiTextRecordWriterSelector<K, V> selector = (MultiTextRecordWriterSelector<K, V>) ReflectionUtils
				.newInstance(selectorClass, conf);

		return new MultiTextRecordWriter<K, V>(outStreams, keyValueSeparator,
				selector);
	}

	/**
	 * Add an output directory path
	 * 
	 * @param job
	 * @param outputDir
	 */
	public static void addOutputPath(Job job, Path outputDir) {
		String dirs = job.getConfiguration().get(OUTPUT_DIR);
		job.getConfiguration().set(
				OUTPUT_DIR,
				dirs == null ? outputDir.toString() : dirs + ","
						+ outputDir.toString());
	}

	/**
	 * Get the output directory paths
	 * 
	 * @param job
	 * @return a list of paths
	 */
	public static Path[] getOutputPaths(JobContext job) {
		String dirs = job.getConfiguration().get(OUTPUT_DIR, "");
		String[] list = StringUtils.split(dirs);
		Path[] result = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			result[i] = new Path(list[i]);
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public static void setRecordWriterSelectorClass(JobContext job,
			Class<? extends MultiTextRecordWriterSelector> cls) {
		job.getConfiguration().setClass(RECORD_WRITE_SELECTOR, cls,
				MultiTextRecordWriterSelector.class);
	}

	@SuppressWarnings("unchecked")
	public static Class<? extends MultiTextRecordWriterSelector> getRecordWriterSelectorClass(
			JobContext job) {
		return job.getConfiguration().getClass(RECORD_WRITE_SELECTOR,
				MultiTextRecordWriterSelector.class,
				MultiTextRecordWriterSelector.class);
	}

	/**
	 * Get the default paths and filenames for the output format.
	 * 
	 * @param context
	 *            the task context
	 * @param extension
	 *            an extension to add to the filename
	 * @return a full path $output/_temporary/$taskid/part-[mr]-$id
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private Path[] getDefaultWorkFiles(TaskAttemptContext context,
			String extension) throws IOException, InterruptedException {

		// Get the committers
		MultiTextOutputCommitter committer = (MultiTextOutputCommitter) getOutputCommitter(context);
		FileOutputCommitter[] committers = committer.getCommitters();
		if (committer == null || committers.length == 0)
			return null;

		// Build the paths
		Path[] paths = new Path[committers.length];
		for (int i = 0; i < committers.length; ++i) {
			paths[i] = new Path(committers[i].getWorkPath(), FileOutputFormat
					.getUniqueFile(context, "part", extension));
		}

		return paths;
	}

}
