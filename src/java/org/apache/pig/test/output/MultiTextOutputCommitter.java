package org.apache.pig.test.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * An output committer to wrap multiple file output committers.
 * 
 * @author hero
 */
public class MultiTextOutputCommitter extends OutputCommitter {

	private FileOutputCommitter[] committers;

	/**
	 * Constructor
	 */
	public MultiTextOutputCommitter(Path[] outputPaths,
			TaskAttemptContext context) throws IOException {

		// Create a file output committer for each file
		committers = null;
		if (outputPaths != null && outputPaths.length != 0) {
			committers = new FileOutputCommitter[outputPaths.length];

			for (int i = 0; i < outputPaths.length; ++i) {
				committers[i] = new FileOutputCommitter(outputPaths[i], context);
			}
		}

	}

	/**
	 * @return the committers
	 */
	public FileOutputCommitter[] getCommitters() {
		return committers;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.OutputCommitter#abortTask(org.apache.hadoop
	 * .mapreduce.TaskAttemptContext)
	 */
	@Override
	public void abortTask(TaskAttemptContext arg0) throws IOException {
		if (committers != null) {
			for (FileOutputCommitter committer : committers) {
				committer.abortTask(arg0);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.OutputCommitter#cleanupJob(org.apache.hadoop
	 * .mapreduce.JobContext)
	 */
	@Override
	public void cleanupJob(JobContext arg0) throws IOException {
		if (committers != null) {
			for (FileOutputCommitter committer : committers) {
				committer.cleanupJob(arg0);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.OutputCommitter#commitTask(org.apache.hadoop
	 * .mapreduce.TaskAttemptContext)
	 */
	@Override
	public void commitTask(TaskAttemptContext arg0) throws IOException {
		if (committers != null) {
			for (FileOutputCommitter committer : committers) {
				committer.commitTask(arg0);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.OutputCommitter#needsTaskCommit(org.apache
	 * .hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
		if (committers != null) {
			for (FileOutputCommitter committer : committers) {
				if (committer.needsTaskCommit(arg0))
					return true;
			}
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.OutputCommitter#setupJob(org.apache.hadoop
	 * .mapreduce.JobContext)
	 */
	@Override
	public void setupJob(JobContext arg0) throws IOException {
		if (committers != null) {
			for (FileOutputCommitter committer : committers) {
				committer.setupJob(arg0);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.OutputCommitter#setupTask(org.apache.hadoop
	 * .mapreduce.TaskAttemptContext)
	 */
	@Override
	public void setupTask(TaskAttemptContext arg0) throws IOException {
		if (committers != null) {
			for (FileOutputCommitter committer : committers) {
				committer.setupTask(arg0);
			}
		}
	}

}
