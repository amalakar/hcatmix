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

import org.perf4j.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class Worker implements Callable<SortedMap<Long, List<StopWatchWritable>>> {
    private final TimeKeeper timeKeeper;
    private final List<Task> tasks;
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    public Worker(final TimeKeeper timeKeeper, List<Task> tasks) {
        this.timeKeeper = timeKeeper;
        this.tasks = tasks;
    }

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
                task.doTask();
                stopWatch.stop();
                stopWatches.add(StopWatchWritable.fromStopWatch(stopWatch));

                if(timeKeeper.hasExpired()) {
                    LOG.info(Thread.currentThread().getName() + ": Stopped doing work as thread expired");
                    break workLoop;
                }
            }
        }
        for (Task task : tasks) {
            task.close();
        }
        return timeSeriesStopWatches;
    }
}
