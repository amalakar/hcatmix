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
import org.apache.hcatalog.hcatmix.load.hadoop.IntervalResult;
import org.apache.hcatalog.hcatmix.load.hadoop.StopWatchWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.hcatalog.hcatmix.load.HadoopLoadGenerator.Conf;

/**
 * The mapper task that runs multples threads in parallel and executes the {@link org.apache.hcatalog.hcatmix.load.tasks.Task} provided in
 * JobConf. It records time taken in milliseconds for each task and emits them against timestamp.
 * The output of the mapper is <br/>
 *          Key: Timestamp in minutes <br/>
 *          Value: The result of the map (which is consists of stopwatches/number of threads for the period
 */
public class HCatMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, LongWritable, IntervalResult> {
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
                    OutputCollector<LongWritable, IntervalResult> collector,
                    final Reporter reporter) throws IOException {
        LOG.info(MessageFormat.format("Input: {0}={1}", longWritable, text));
        final List<Future<SortedMap<Long, IntervalResult>>> futures =
                new ArrayList<Future<SortedMap<Long, IntervalResult>>>();

        // Initialize tasks
        List<org.apache.hcatalog.hcatmix.load.tasks.Task> tasks;
        try {
            tasks = initializeTasks(jobConf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ThreadCreatorTimer createNewThreads =
                new ThreadCreatorTimer(new TimeKeeper(timeKeeper), tasks, threadIncrementCount, futures, reporter);

        // Create timer thread to automatically keep on increasing threads at fixed interval
        Timer newThreadCreator = new Timer(true);
        newThreadCreator.scheduleAtFixedRate(createNewThreads, 0, threadIncrementIntervalInMillis);

        // Sleep and let the tasks get expired
        try {
            Thread.sleep(timeKeeper.getRemainingTimeIncludingBuffer());
        } catch (InterruptedException e) {
            LOG.error("Got interrupted while sleeping for timer thread to finish");
        }
        newThreadCreator.cancel();
        LOG.info("Time is over, will collect the futures now. Total number of threads: " + futures.size());
        SortedMap<Long, IntervalResult> stopWatchAggregatedTimeSeries =
                new TreeMap<Long, IntervalResult>();

        // Merge the corresponding time interval results received from all the threads for each time interval
        for (Future<SortedMap<Long, IntervalResult>> future : futures) {
            try {
                SortedMap<Long, IntervalResult> threadTimeSeries = future.get();
                for (Map.Entry<Long, IntervalResult> entry : threadTimeSeries.entrySet()) {
                    Long timeStamp = entry.getKey();
                    IntervalResult intervalResult = entry.getValue();

                    if(stopWatchAggregatedTimeSeries.containsKey(timeStamp)) {
                        stopWatchAggregatedTimeSeries.get(timeStamp).addIntervalResult(intervalResult);
                    } else {
                        stopWatchAggregatedTimeSeries.put(timeStamp, intervalResult);
                    }
                    LOG.info(MessageFormat.format("{0}: Added {1} stopwatches. Current stopwatch number: {2}",
                            timeStamp, intervalResult.getStopWatchList().size(),
                            stopWatchAggregatedTimeSeries.get(timeStamp).getStopWatchList().size()));
                }
            } catch (Exception e) {
                LOG.error("Error while getting thread results", e);
            }
        }

        // Output the consolidated futures for this map along with the number of threads against time
        LOG.info("Collected all the statistics for #threads: " + createNewThreads.getThreadCount());
        SortedMap<Long, Integer> threadCountTimeSeries = createNewThreads.getThreadCountTimeSeries();
        int threadCount = 0;
        for (Map.Entry<Long, IntervalResult> entry : stopWatchAggregatedTimeSeries.entrySet()) {
            long timeStamp = entry.getKey();
            IntervalResult intervalResult = entry.getValue();
            if(threadCountTimeSeries.containsKey(timeStamp)) {
                threadCount = threadCountTimeSeries.get(timeStamp);
            }
            intervalResult.setThreadCount(threadCount);
            collector.collect(new LongWritable(timeStamp), intervalResult);
        }
    }

    /**
     * Creates the {@link org.apache.hcatalog.hcatmix.load.tasks.Task} instances using reflection and calls configure on it, The task names
     * are comma separated list of {@link org.apache.hcatalog.hcatmix.load.tasks.Task} classes.
     * @param jobConf
     * @return
     * @throws Exception
     */
    private List<org.apache.hcatalog.hcatmix.load.tasks.Task> initializeTasks(JobConf jobConf) throws Exception {
        String classNames = jobConf.get(Conf.TASK_CLASS_NAMES.getJobConfKey());
        if(StringUtils.isEmpty(classNames)) {
            String msg = MessageFormat.format("{0} setting is found to be null/empty", Conf.TASK_CLASS_NAMES);
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        List<org.apache.hcatalog.hcatmix.load.tasks.Task> tasks = new ArrayList<org.apache.hcatalog.hcatmix.load.tasks.Task>();

        String[] classes = classNames.split(",");
        for (String aClass : classes) {
            Class clazz = Class.forName(aClass);
            try {
                org.apache.hcatalog.hcatmix.load.tasks.Task task = (org.apache.hcatalog.hcatmix.load.tasks.Task) clazz.newInstance();
                task.configure(jobConf);
                tasks.add(task);
            } catch (Exception e) {
                LOG.info("Couldn't instantiate class:" + aClass, e);
                throw e;
            }
        }
        return tasks;
    }
}
