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

import org.apache.hcatalog.hcatmix.load.hadoop.StopWatchWritable;
import org.apache.hcatalog.hcatmix.load.tasks.Task;
import org.perf4j.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

/**
 * The executor for {@link org.apache.hcatalog.hcatmix.load.tasks.Task}, this class also maintains the time taken for doing tasks using a timeseries.
 * The method stops doing task and returns once the expiry time is over.
 */
public class TaskExecutor implements Callable<SortedMap<Long, List<StopWatchWritable>>> {
    private final TimeKeeper timeKeeper;
    private final List<Task> tasks;
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

    public TaskExecutor(final TimeKeeper timeKeeper, List<Task> tasks) {
        this.timeKeeper = timeKeeper;
        this.tasks = tasks;
    }

    /**
     * Measures time taken for the task over time. It bookkeeps the StopWatches against time interval as configured in
     * {@link TimeKeeper}
     * @return a SortedMap of list of stopwatches against timeStamps
     * @throws Exception
     */
    @Override
    public SortedMap<Long, List<StopWatchWritable>> call() throws Exception {
        SortedMap<Long, List<StopWatchWritable>> timeSeriesStopWatches = new TreeMap<Long, List<StopWatchWritable>>();

        List<StopWatchWritable> stopWatches = new ArrayList<StopWatchWritable>();
        timeKeeper.updateCheckpoint();
        workLoop: while(true) {
            for (Task task : tasks) {
                if(timeKeeper.hasNextCheckpointArrived()) {
                    timeSeriesStopWatches.put(timeKeeper.getCurrentCheckPoint(), stopWatches);
                    stopWatches = new ArrayList<StopWatchWritable>();
                    timeKeeper.updateCheckpoint();
                }

                StopWatch stopWatch = new StopWatch(task.getName());
                StopWatch stopWatchFromTask = null;
                boolean errorOccured = false;
                try {
                    stopWatchFromTask = task.doTask();
                } catch (Exception e) {
                    LOG.info("Error encountered while doing task", e);
                    errorOccured = true;
                }
                stopWatch.stop();

                if (!errorOccured) {
                    // Give preference if the task itself returns a StopWatch otherwise use the one we calculated
                    if (stopWatchFromTask != null) {
                        stopWatch = stopWatchFromTask;
                    }
                    stopWatches.add(StopWatchWritable.fromStopWatch(stopWatch));
                }

                if(timeKeeper.hasExpired()) {
                    LOG.info(Thread.currentThread().getName() + ": Stopped doing work as thread expired");
                    break workLoop;
                }
            }
        }
        for (Task task : tasks) {
            LOG.info("Errors for the task " + task.getName() + " is: " + task.getNumErrors()); // TODO incorporate this in final stats
            task.close();
        }
        return timeSeriesStopWatches;
    }
}
