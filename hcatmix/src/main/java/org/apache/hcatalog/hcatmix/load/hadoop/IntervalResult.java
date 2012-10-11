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

package org.apache.hcatalog.hcatmix.load.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writable object representing statistics/number of errors etc. for an interval of time.
 */
public class IntervalResult implements Writable {
    private Integer threadCount;
    private Integer numErrors; // TODO: Should be a map
    // Stop Watch collected during the period
    private List<StopWatchWritable> stopWatchList;

    public IntervalResult() {
    }

    public IntervalResult(Integer numErrors, List<StopWatchWritable> stopWatchList) {
        this(null, numErrors, stopWatchList);
    }

    public IntervalResult(Integer threadCount, Integer numErrors, List<StopWatchWritable> stopWatchList) {
        this.threadCount = threadCount;
        this.numErrors = numErrors;
        this.stopWatchList = stopWatchList;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(stopWatchList.size());
        for (StopWatchWritable stopWatchWritable : stopWatchList) {
            stopWatchWritable.write(dataOutput);
        }
        dataOutput.writeInt(threadCount);
        dataOutput.writeInt(numErrors);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        stopWatchList = new ArrayList<StopWatchWritable>();
        for (int i = 0; i < size; i++) {
            StopWatchWritable stopWatch = new StopWatchWritable();
            stopWatch.readFields(dataInput);
            stopWatchList.add(stopWatch);
        }

        threadCount = dataInput.readInt();
        numErrors = dataInput.readInt();
    }

    public Integer getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(Integer threadCount) {
        this.threadCount = threadCount;
    }

    public List<StopWatchWritable> getStopWatchList() {
        return stopWatchList;
    }

    public Integer getNumErrors() {
        return numErrors;
    }

    public void addIntervalResult(IntervalResult intervalResult) {
        numErrors += intervalResult.getNumErrors();
        stopWatchList.addAll(intervalResult.getStopWatchList());
    }
}
