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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;

/**
* Author: malakar
*/
public class HCatMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, StopWatchWritable.ArrayStopWatchWritable> {
    public static final int THREAD_INCREMENT_COUNT = 5;
    public static final long THREAD_INCREMENT_INTERVAL = 5 * 60 * 1000;
    private static final int MAP_TIMEOUT_MINUTES = 3;
    private static final int MAP_TIMEOUT_BUFFER_IN_MINUTES = 1;
    private static final int TIME_SERIES_INTERVAL_IN_MINUTES = 1;
    private static final Logger LOG = LoggerFactory.getLogger(HCatMapper.class);

    private TimeKeeper timeKeeper;
    private Token token;

    public HCatMapper() {
    }

    @Override
    public void configure(JobConf jobConf) {
        super.configure(jobConf);
        timeKeeper = new TimeKeeper(MAP_TIMEOUT_MINUTES, MAP_TIMEOUT_BUFFER_IN_MINUTES, TIME_SERIES_INTERVAL_IN_MINUTES);
        token = jobConf.getCredentials().getToken(new Text(HadoopLoadGenerator.METASTORE_TOKEN_KEY));
        try {
            LOG.info("Kerberos token received from job launcher: " + token.encodeToUrlString());
        } catch (IOException e) {
            LOG.error("Couldn't encode token to URL string");
        }
        try {
            UserGroupInformation.getCurrentUser().addToken(token);
        } catch (IOException e) {
            LOG.info("Error adding token to user", e);
        }
    }

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<LongWritable, StopWatchWritable.ArrayStopWatchWritable> collector, final Reporter reporter) throws IOException {
        LOG.info(MessageFormat.format("Input: {0}={1}", longWritable, text));
        final List<Future<SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable>>> futures = new ArrayList<Future<SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable>>>();
        final List<Task> tasks = new ArrayList<Task>();
        tasks.add(new HCatLoadTask.HCatReadLoadTask(token));

        ThreadCreatorTimer createNewThreads = new ThreadCreatorTimer(timeKeeper, tasks, futures, reporter);

        Timer newThreadCreator = new Timer(true);
        newThreadCreator.scheduleAtFixedRate(createNewThreads, 0, THREAD_INCREMENT_INTERVAL);
        try {
            Thread.sleep(timeKeeper.getRemainingTimeIncludingBuffer());
        } catch (InterruptedException e) {
            LOG.error("Got interrupted while sleeping for timer thread to finish");
        }
        newThreadCreator.cancel();
        LOG.info("Time is over, will collect the futures now");
        SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable> stopWatches = new TreeMap<Long, StopWatchWritable.ArrayStopWatchWritable>();
        for (Future<SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable>> future : futures) {
            try {
                stopWatches.putAll(future.get());
            } catch (Exception e) {
                LOG.error("Error while getting thread results", e);
            }
        }
        LOG.info("Collected all the statistics for #threads: " + createNewThreads.getThreadCount());
        for (Map.Entry<Long, StopWatchWritable.ArrayStopWatchWritable> entry : stopWatches.entrySet()) {
            collector.collect(new LongWritable(entry.getKey()), entry.getValue());
        }
    }

}
