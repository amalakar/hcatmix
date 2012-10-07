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

package org.apache.hcatalog.hcatmix.load;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.perf4j.StopWatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* Author: malakar
*/
public class StopWatchWritable implements Writable {
    private StopWatch stopWatch;

    public StopWatchWritable() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(stopWatch.getStartTime());
        dataOutput.writeLong(stopWatch.getElapsedTime());
        dataOutput.writeUTF(stopWatch.getTag());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        long startTime = dataInput.readLong();
        long elapsedTime = dataInput.readLong();
        String tag = dataInput.readUTF();
        stopWatch = new StopWatch(startTime, elapsedTime, tag, null);
    }

    public StopWatchWritable(StopWatch stopWatch) {
        this.stopWatch = stopWatch;
    }

    public static StopWatchWritable fromStopWatch(StopWatch stopWatch) {
        return new StopWatchWritable(stopWatch);
    }

    public StopWatch getStopWatch() {
        return  stopWatch;
    }

    public static class ArrayStopWatchWritable extends ArrayWritable {
        public ArrayStopWatchWritable() {
            super(StopWatchWritable.class);
        }

        public ArrayStopWatchWritable(StopWatchWritable[] values) {
            super(StopWatchWritable.class, values);
        }
    }
}
