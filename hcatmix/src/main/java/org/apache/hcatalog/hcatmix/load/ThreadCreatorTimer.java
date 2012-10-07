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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
* Author: malakar
*/
public class ThreadCreatorTimer extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadCreatorTimer.class);
    private int threadCount;
    private final TimeKeeper timeKeeper;
    private final List<Task> tasks;
    private final List<Future<SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable>>> futures;
    private final Reporter reporter;
    enum COUNTERS { NUM_THREADS}

    public ThreadCreatorTimer(TimeKeeper timeKeeper, List<Task> tasks,
                              List<Future<SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable>>> futures, Reporter reporter) {
        this.timeKeeper = timeKeeper;
        this.tasks = tasks;
        this.futures = futures;
        this.reporter = reporter;
        threadCount = 0;
    }

    public void run() {
        LOG.info("About to create " + HCatMapper.THREAD_INCREMENT_COUNT + " threads.");
        final ExecutorService executorPool = Executors.newFixedThreadPool(HCatMapper.THREAD_INCREMENT_COUNT);
        Collection<MetaStoreWorker> workers = new ArrayList<MetaStoreWorker>(HCatMapper.THREAD_INCREMENT_COUNT);
        for (int i = 0; i < HCatMapper.THREAD_INCREMENT_COUNT; i++) {
            workers.add(new MetaStoreWorker(timeKeeper, tasks));
        }

        for (MetaStoreWorker worker : workers) {
            futures.add(executorPool.submit(worker));
        }
        threadCount += HCatMapper.THREAD_INCREMENT_COUNT;
        LOG.info("Current number of threads: " + threadCount);
        reporter.progress();
        reporter.setStatus(MessageFormat.format("#Threads: {0}, Progress: {1}%", threadCount, timeKeeper.getPercentageProgress()));
        reporter.incrCounter(COUNTERS.NUM_THREADS, HCatMapper.THREAD_INCREMENT_COUNT);
    }

    public int getThreadCount() {
        return threadCount;
    }
}
