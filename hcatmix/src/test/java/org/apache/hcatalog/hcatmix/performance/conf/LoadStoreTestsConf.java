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

package org.apache.hcatalog.hcatmix.performance.conf;

import java.util.List;

/**
 * Configuration for all the load/store configuration stored in YAML files
 */
public class LoadStoreTestsConf {
    private List<LoadStoreTestConf> tests;
    private int maxMaps;
    private int defaultNumRuns;

    public List<LoadStoreTestConf> getTests() {
        return tests;
    }

    public void setTests(List<LoadStoreTestConf> tests) {
        this.tests = tests;
    }

    public int getMaxMaps() {
        return maxMaps;
    }

    public void setMaxMaps(int maxMaps) {
        this.maxMaps = maxMaps;
    }

    public int getDefaultNumRuns() {
        return defaultNumRuns;
    }

    public void setDefaultNumRuns(int defaultNumRuns) {
        this.defaultNumRuns = defaultNumRuns;
    }
}
