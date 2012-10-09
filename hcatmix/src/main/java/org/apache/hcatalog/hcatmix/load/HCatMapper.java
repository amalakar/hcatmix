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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.hcatmix.load.hadoop.MapResult;
import org.apache.hcatalog.hcatmix.load.hadoop.StopWatchWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.hcatalog.hcatmix.load.HadoopLoadGenerator.Conf;

public class HCatMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, LongWritable, MapResult> {
    private static final Logger LOG = LoggerFactory.getLogger(HCatMapper.class);

    private int threadIncrementCount;
    private long threadIncrementIntervalInMillis;
    private JobConf jobConf;

    private TimeKeeper timeKeeper;

    public HCatMapper() {
    }

    @Override
    public void configure(JobConf jobConf) {
        super.configure(jobConf);
        this.jobConf = jobConf;

        final int mapRunTime = getFromJobConf(Conf.MAP_RUN_TIME_MINUTES);
        final int timeSeriesIntervalInMinutes = getFromJobConf(Conf.STAT_COLLECTION_INTERVAL_MINUTE);
        final int mapRuntimeExtraBufferInMinutes = getFromJobConf(Conf.THREAD_COMPLETION_BUFFER_MINUTES);

        threadIncrementCount = getFromJobConf(Conf.THREAD_INCREMENT_COUNT);
        threadIncrementIntervalInMillis = getFromJobConf(Conf.THREAD_INCREMENT_INTERVAL_MINUTES) * 60 * 1000;

        timeKeeper = new TimeKeeper(mapRunTime, mapRuntimeExtraBufferInMinutes,
                timeSeriesIntervalInMinutes);

    }

    private int getFromJobConf(Conf conf) {
        int value = jobConf.getInt(conf.getJobConfKey(), conf.defaultValue);
        LOG.info(conf.getJobConfKey() + " value is: " + conf.defaultValue);
        return value;
    }

    @Override
    public void map(LongWritable longWritable, Text text,
                    OutputCollector<LongWritable, MapResult> collector,
                    final Reporter reporter) throws IOException {
        LOG.info(MessageFormat.format("Input: {0}={1}", longWritable, text));
        final List<Future<SortedMap<Long, List<StopWatchWritable>>>> futures =
                new ArrayList<Future<SortedMap<Long, List<StopWatchWritable>>>>();

        List<Task> tasks;
        try {
            tasks = initializeTasks(jobConf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ThreadCreatorTimer createNewThreads =
                new ThreadCreatorTimer(new TimeKeeper(timeKeeper), tasks, threadIncrementCount, futures, reporter);

        Timer newThreadCreator = new Timer(true);
        newThreadCreator.scheduleAtFixedRate(createNewThreads, 0, threadIncrementIntervalInMillis);
        try {
            Thread.sleep(timeKeeper.getRemainingTimeIncludingBuffer());
        } catch (InterruptedException e) {
            LOG.error("Got interrupted while sleeping for timer thread to finish");
        }
        newThreadCreator.cancel();
        LOG.info("Time is over, will collect the futures now. Total number of threads: " + futures.size());
        SortedMap<Long, List<StopWatchWritable>> stopWatchAggregatedTimeSeries =
                new TreeMap<Long, List<StopWatchWritable>>();
        for (Future<SortedMap<Long, List<StopWatchWritable>>> future : futures) {
            try {
                SortedMap<Long, List<StopWatchWritable>> threadTimeSeries = future.get();
                for (Map.Entry<Long, List<StopWatchWritable>> entry : threadTimeSeries.entrySet()) {
                    Long timeStamp = entry.getKey();
                    List<StopWatchWritable> threadStopWatches = entry.getValue();

                    if(stopWatchAggregatedTimeSeries.containsKey(timeStamp)) {
                        stopWatchAggregatedTimeSeries.get(timeStamp).addAll(threadStopWatches);
                    } else {
                        stopWatchAggregatedTimeSeries.put(timeStamp, threadStopWatches);
                    }
                    LOG.info(MessageFormat.format("{0}: Added {1} stopwatches. Current stopwatch number: {2}",
                            timeStamp, threadStopWatches.size(), stopWatchAggregatedTimeSeries.get(timeStamp).size()));
                }
            } catch (Exception e) {
                LOG.error("Error while getting thread results", e);
            }
        }
        LOG.info("Collected all the statistics for #threads: " + createNewThreads.getThreadCount());
        SortedMap<Long, Integer> threadCountTimeSeries = createNewThreads.getThreadCountTimeSeries();
        int threadCount = 0;
        for (Map.Entry<Long, List<StopWatchWritable>> entry : stopWatchAggregatedTimeSeries.entrySet()) {
            long timeStamp = entry.getKey();
            List<StopWatchWritable> stopWatchList = entry.getValue();
            if(threadCountTimeSeries.containsKey(timeStamp)) {
                threadCount = threadCountTimeSeries.get(timeStamp);
            }
            collector.collect(new LongWritable(timeStamp),
                    new MapResult(threadCount, stopWatchList));
        }
    }

    private List<Task> initializeTasks(JobConf jobConf) throws Exception {
        String classNames = jobConf.get(Conf.TASK_CLASS_NAMES.getJobConfKey());
        if(StringUtils.isEmpty(classNames)) {
            String msg = MessageFormat.format("{0} setting is found to be null/empty", Conf.TASK_CLASS_NAMES);
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        List<Task> tasks = new ArrayList<Task>();

        String[] classes = classNames.split(",");
        for (String aClass : classes) {
            Class clazz = Class.forName(aClass);
            try {
                Task task = (Task) clazz.newInstance();
                task.configure(jobConf);
                tasks.add(task);
            } catch (Exception e) {
                LOG.info("Couldn't instantiate class: aClass", e);
                throw e;
            }
        }
        return tasks;
    }
}
