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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;

public class HCatReducer extends MapReduceBase implements
        Reducer<LongWritable, StopWatchWritable.MapResult,
        LongWritable, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(HCatReducer.class);

    public HCatReducer() {
    }

    @Override
    public void reduce(LongWritable timeStamp, Iterator<StopWatchWritable.MapResult> mapResultIterator,
                       OutputCollector<LongWritable, Text> collector, Reporter reporter)
            throws IOException {
        GroupedTimingStatistics statistics = new GroupedTimingStatistics();
        LOG.info(MessageFormat.format("Going through statistics for time: {0}", timeStamp));
        int threadCount = 0;
        while (mapResultIterator.hasNext()) {
            StopWatchWritable.MapResult mapResult = mapResultIterator.next();
            List<StopWatchWritable> stopWatches = mapResult.getStopWatchList();
            for (StopWatchWritable stopWatch : stopWatches) {
                statistics.addStopWatch(stopWatch.getStopWatch());
            }
            LOG.info(MessageFormat.format("Reducing for {0} Stopwatch count: {1}", timeStamp,
                    mapResult.getStopWatchList().size()));
            LOG.info("Current statistics: " + statistics);
            threadCount += mapResult.getThreadCount();
        }
        LOG.info(MessageFormat.format("Final statistics for {0}: Threads: {1}, Statistics: {2}",
                timeStamp, threadCount, statistics));
        collector.collect(timeStamp, new Text(statistics.toString() + "\n threadCount " + threadCount));
    }
}
