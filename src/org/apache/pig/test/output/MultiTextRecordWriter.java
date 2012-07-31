package org.apache.pig.test.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A record writer that wraps multiple record writers. The selection of the
 * writer is based on a user-provided RecordWriterSelector
 * 
 * @author hero
 * 
 */
public class MultiTextRecordWriter<K, V> extends RecordWriter<K, V> {

	private ArrayList<LineRecordWriter<K, V>> writers = null;
	private MultiTextRecordWriterSelector<K, V> selector = null;

	/**
	 * Constructor
	 * 
	 * @param outStreams
	 * @param keyValueSeparator
	 */
	public MultiTextRecordWriter(DataOutputStream outStreams[],
			String keyValueSeparator,
			MultiTextRecordWriterSelector<K, V> selector) {
		if (outStreams != null && outStreams.length != 0) {
			writers = new ArrayList<LineRecordWriter<K, V>>(outStreams.length);

			for (DataOutputStream out : outStreams) {
				writers.add(new LineRecordWriter<K, V>(out, keyValueSeparator));
			}
		}

		this.selector = selector;
	}

	@Override
	public synchronized void close(TaskAttemptContext context)
			throws IOException, InterruptedException {
		if (writers != null) {
			for (LineRecordWriter<K, V> writer : writers) {
				writer.close(context);
			}
		}
	}

	@Override
	public synchronized void write(K key, V value) throws IOException,
			InterruptedException {
		if (writers != null) {
			int i = selector.getWriterIndex(key, value);
			if (i < writers.size()) {
				writers.get(i).write(selector.generateActualKey(key, value),
						selector.generateActualValue(key, value));
			}
		}
	}

	/**
	 * LineRecordWriter
	 * 
	 * @param <K>
	 * @param <V>
	 */
	protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {

		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		protected DataOutputStream out;
		private final byte[] keyValueSeparator;

		/**
		 * Constructor
		 * 
		 * @param out
		 * @param keyValueSeparator
		 */
		public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
			this.out = out;
			try {
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		/**
		 * Constructor
		 * 
		 * @param out
		 */
		public LineRecordWriter(DataOutputStream out) {
			this(out, "\t");
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		@Override
		public synchronized void write(K key, V value) throws IOException {

			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}
			if (!nullKey) {
				writeObject(key);
			}
			if (!(nullKey || nullValue)) {
				out.write(keyValueSeparator);
			}
			if (!nullValue) {
				writeObject(value);
			}
			out.write(newline);
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			out.close();
		}
	}

}
