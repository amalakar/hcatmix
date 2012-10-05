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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.perf4j.GroupedTimingStatistics;
import org.perf4j.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Author: malakar
 */
public class HCatReducer extends MapReduceBase implements Reducer<LongWritable, List<StopWatch>, LongWritable, GroupedTimingStatistics> {
    private static final Logger LOG = LoggerFactory.getLogger(HCatReducer.class);

    @Override
    public void reduce(LongWritable time, Iterator<List<StopWatch>> stats, OutputCollector<LongWritable, GroupedTimingStatistics> collector, Reporter reporter) throws IOException {
        GroupedTimingStatistics statistics = new GroupedTimingStatistics();
        LOG.info("Going through statistics for time: " + time);
        while (stats.hasNext()) {
            List<StopWatch> stat = stats.next();
            statistics.addStopWatches(stat);
            LOG.info("Stats:" + stat);
        }
        LOG.info("Final statistics for " + time + " " + statistics);
        collector.collect(null, statistics);
    }
}
