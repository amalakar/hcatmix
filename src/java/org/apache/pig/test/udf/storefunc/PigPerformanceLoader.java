/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test.udf.storefunc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.LoadCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.DataType;
import org.apache.pig.test.utils.datagen.Writer;

/**
 * A load function for the performance tests.
 */
public class PigPerformanceLoader extends PigStorage {

    BagFactory bagFactory;
    TupleFactory tupleFactory;

    public PigPerformanceLoader() {
        // Assume ^A as a delimiter
        super("");
        bagFactory = BagFactory.getInstance();
        tupleFactory = TupleFactory.getInstance();
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return new Caster();
    }
        
    class Caster implements LoadCaster {
        
        Utf8StorageConverter helper = new Utf8StorageConverter();
        /**
         * 
         */
        public Caster() {
            // TODO Auto-generated constructor stub
        }
        
        public DataBag bytesToBag(byte[] b, ResourceFieldSchema fs) throws IOException {
            if (b == null) return null;

            DataBag bag = bagFactory.newDefaultBag();

            int pos = 0;
            while (pos < b.length) {
                Tuple t = tupleFactory.newTuple(1);

                // Figure out how long until the next element in the list.
                int start = pos;
                while (pos < b.length && b[pos] != 2) pos++; // 2 is ^B

                byte[] copy = new byte[pos - start];
                int i, j;
                for (i = start + 1, j = 0; i < pos; i++, j++) copy[j] = b[i];

                // The first byte will tell us what type the field is.
                try {
                    switch (DataType.fromInt(b[start])) {
                        case INT: t.set(0, bytesToInteger(copy)); break;
                        case LONG: t.set(0, bytesToLong(copy)); break;
                        case FLOAT: t.set(0, bytesToFloat(copy)); break;
                        case DOUBLE: t.set(0, bytesToDouble(copy)); break;
                        case STRING: t.set(0, bytesToCharArray(copy)); break;
                        case MAP: t.set(0, bytesToMap(copy)); break;
                        case BAG: t.set(0, bytesToBag(copy, null)); break;
                        default: throw new RuntimeException("unknown type " + b[start]);
                    }
                } catch (ExecException ee) {
                    throw new IOException(ee);
                }
                pos++; // move past the separator
                bag.add(t);
            }

            return bag;
        }

		public Map<String, Object> bytesToMap(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
			return bytesToMap(b);
		}

        public Map<String, Object> bytesToMap(byte[] b) throws IOException {
            if (b == null) return null;

            Map<String, Object> m = new HashMap<String, Object>(26);

            int pos = 0;
            while (pos < b.length) {

                // The key is always one character at the moment.
                byte[] k = new byte[1];
                k[0] = b[pos];
                String key = new String(k);
                pos += 2;
                int start = pos;
                while (pos < b.length && b[pos] != (int) Writer.CTRL_C) pos++;

                byte[] copy = new byte[pos - start];
                int i, j;
                for (i = start + 1, j = 0; i < pos; i++, j++) copy[j] = b[i];
                String val = bytesToCharArray(copy);
                m.put(key, val);
                pos++; // move past ^C
            }
            return m; 
        }

        @Override
        public String bytesToCharArray(byte[] arg0) throws IOException {
            return helper.bytesToCharArray(arg0);
        }

        @Override
        public Double bytesToDouble(byte[] arg0) throws IOException {
            return helper.bytesToDouble(arg0);
        }

        @Override
        public Float bytesToFloat(byte[] arg0) throws IOException {
            return helper.bytesToFloat(arg0);
        }

        @Override
        public Integer bytesToInteger(byte[] arg0) throws IOException {
            return helper.bytesToInteger(arg0);
        }

        @Override
        public Long bytesToLong(byte[] arg0) throws IOException {
            return helper.bytesToLong(arg0);
        }

        @Override
        public Tuple bytesToTuple(byte[] arg0, ResourceFieldSchema fs) throws IOException {
            return helper.bytesToTuple(arg0, fs);
        }
        
    }

}
