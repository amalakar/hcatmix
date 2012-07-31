package org.apache.pig.test.output;

/**
 * This class is used by the MultiTextRecordWriter to decide where to direct the
 * job output.
 * 
 * @author hero
 */
public class MultiTextRecordWriterSelector<K, V> {

	public MultiTextRecordWriterSelector() {
	}

	/**
	 * Get the writer's index based on the key and the value
	 * 
	 * @param key
	 *            the key of the output data
	 * @param value
	 *            the value of the output data
	 * @return the index of the writer
	 */
	protected int getWriterIndex(K key, V value) {
		return 0;
	}

	/**
	 * Generate the actual key from the given key/value. The default behavior is
	 * that the actual key is equal to the given key
	 * 
	 * @param key
	 *            the key of the output data
	 * @param value
	 *            the value of the output data
	 * @return the actual key derived from the given key/value
	 */
	protected K generateActualKey(K key, V value) {
		return key;
	}

	/**
	 * Generate the actual value from the given key and value. The default
	 * behavior is that the actual value is equal to the given value
	 * 
	 * @param key
	 *            the key of the output data
	 * @param value
	 *            the value of the output data
	 * @return the actual value derived from the given key/value
	 */
	protected V generateActualValue(K key, V value) {
		return value;
	}

}
