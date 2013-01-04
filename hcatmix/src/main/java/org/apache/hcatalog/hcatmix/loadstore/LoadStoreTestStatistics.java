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

package org.apache.hcatalog.hcatmix.loadstore;


import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds statistics for load/store tests of a hcatSpecFile
 */
public class LoadStoreTestStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(LoadStoreTestStatistics.class);
    private String fileName;
    private GroupedTimingStatistics timedStats;
    private String chartUrl;

    public LoadStoreTestStatistics(String fileName, GroupedTimingStatistics timedStats) {
        this.fileName = fileName;
        this.timedStats = timedStats;
        chartUrl = LoadStoreTestGrapher.getChartURL(fileName, timedStats); //TODO remove this
    }

    public String getChartUrl() {
        return chartUrl;
    }

    public GroupedTimingStatistics getTimedStats() {
        return timedStats;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public String toString() {
        return fileName + ":\n" + timedStats + "\n" + getChartUrl();
    }
}
