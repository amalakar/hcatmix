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

import org.apache.hcatalog.hcatmix.publisher.LoadStoreResultsPublisher;
import org.apache.hcatalog.hcatmix.publisher.ResultsPublisher;
import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: malakar
 */
public class LoadStoreTestResults {
    private Map<String, LoadStoreStats> results;
    private static final Logger LOG = LoggerFactory.getLogger(LoadStoreStats.class);

    public LoadStoreTestResults() {
        results = new HashMap<String, LoadStoreStats>();
    }

    public void addResult(String fileName, GroupedTimingStatistics stats) {
        LOG.info(fileName + " Statistics:\n" + stats.toString());
        results.put(fileName, new LoadStoreStats(new File(fileName).getName(), stats));
    }

    public void publish() throws Exception {
        for (Map.Entry<String, LoadStoreStats> hCatStatsEntry : results.entrySet()) {
            String fileName = hCatStatsEntry.getKey();
            LoadStoreStats stats = hCatStatsEntry.getValue();
            LOG.info(fileName + " Statistics:\n" + stats.toString());
            LOG.info("Chart URL: " + stats.getChartUrl());
        }
        ResultsPublisher publisher = new LoadStoreResultsPublisher(new ArrayList<LoadStoreStats>(results.values()));
        publisher.publishAll();
    }

    public Map<String, LoadStoreStats> getResults() {
        return results;
    }
}
