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

/**
* Author: malakar
*/
public class MetaStoreWorker implements Callable<SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable>> {
    private final TimeKeeper timeKeeper;
    private final List<Task> tasks;
    private static final Logger LOG = LoggerFactory.getLogger(MetaStoreWorker.class);

    public MetaStoreWorker(final TimeKeeper timeKeeper, List<Task> tasks) {
        this.timeKeeper = timeKeeper;
        this.tasks = tasks;
    }

    @Override
    public SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable> call() throws Exception {
        SortedMap<Long, StopWatchWritable.ArrayStopWatchWritable> timeSeriesStopWatches = new TreeMap<Long, StopWatchWritable.ArrayStopWatchWritable>();

        List<StopWatchWritable> stopWatches = new ArrayList<StopWatchWritable>();
        TimeKeeper timeSeriesKeeper = new TimeKeeper(timeKeeper);
        timeSeriesKeeper.updateCheckpoint();
        boolean firstTime = true;
        metastoreCalls: while(true) {
            for (Task task : tasks) {
                if(timeKeeper.hasNextCheckpointArrived()) {
                    if(!firstTime) {
                        StopWatchWritable.ArrayStopWatchWritable arrayStopWatchWritable =
                                new StopWatchWritable.ArrayStopWatchWritable(stopWatches.toArray(new StopWatchWritable[0]));
                        timeSeriesStopWatches.put(timeSeriesKeeper.getCurrentCheckPoint(), arrayStopWatchWritable);
                        firstTime = false;
                    }
                    stopWatches = new ArrayList<StopWatchWritable>();
                    timeKeeper.updateCheckpoint();
                }

                StopWatch stopWatch = new StopWatch(task.getName());
                task.doTask();
                stopWatch.stop();
                stopWatches.add(StopWatchWritable.fromStopWatch(stopWatch));
                if(timeKeeper.hasExpired()) {
                    LOG.info(Thread.currentThread().getName() + ": Stopped doing work as thread expired");
                    break metastoreCalls;
                }
            }
        }
        for (Task task : tasks) {
            task.close();
        }
        return timeSeriesStopWatches;
    }
}
