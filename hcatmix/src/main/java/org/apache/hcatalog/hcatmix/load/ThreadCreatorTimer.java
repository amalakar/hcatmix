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

import org.apache.hadoop.mapred.Reporter;
import org.apache.hcatalog.hcatmix.load.hadoop.StopWatchWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ThreadCreatorTimer extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadCreatorTimer.class);
    private int threadCount;
    private final TimeKeeper timeKeeper;
    private final List<Task> tasks;
    private final List<Future<SortedMap<Long, List<StopWatchWritable>>>> futures;
    private final Reporter reporter;
    private final SortedMap<Long, Integer> threadCountTimeSeries = new TreeMap<Long, Integer>();
    private final int threadIncrementCount;
    enum COUNTERS { NUM_THREADS}

    public ThreadCreatorTimer(TimeKeeper timeKeeper, List<Task> tasks, final int threadIncrementCount,
                              List<Future<SortedMap<Long, List<StopWatchWritable>>>> futures, Reporter reporter) {
        this.timeKeeper = timeKeeper;
        this.tasks = tasks;
        this.threadIncrementCount = threadIncrementCount;
        this.futures = futures;
        this.reporter = reporter;
        threadCount = 0;
        timeKeeper.updateCheckpoint();
    }

    public void run() {
        LOG.info("About to create " + threadIncrementCount + " threads.");
        final ExecutorService executorPool = Executors.newFixedThreadPool(threadIncrementCount);
        Collection<TaskExecutor> taskExecutors = new ArrayList<TaskExecutor>(threadIncrementCount);
        for (int i = 0; i < threadIncrementCount; i++) {
            taskExecutors.add(new TaskExecutor(new TimeKeeper(timeKeeper), tasks));
        }

        for (TaskExecutor taskExecutor : taskExecutors) {
            futures.add(executorPool.submit(taskExecutor));
        }
        threadCount += threadIncrementCount;

        // Reporting
        LOG.info("Current number of threads: " + threadCount);
        reporter.progress();
        final String msg = MessageFormat.format("#Threads: {0}, Progress: {1}%",
                threadCount, timeKeeper.getPercentageProgress());
        LOG.info(msg);
        reporter.setStatus(msg);
        reporter.incrCounter(COUNTERS.NUM_THREADS, threadIncrementCount);

        // Update time series
        if(timeKeeper.hasNextCheckpointArrived()) {
            threadCountTimeSeries.put(timeKeeper.getCurrentCheckPoint(), getThreadCount());
            timeKeeper.updateCheckpoint();
        }
    }

    public int getThreadCount() {
        return threadCount;
    }

    public SortedMap<Long, Integer> getThreadCountTimeSeries() {
        return threadCountTimeSeries;
    }
}
