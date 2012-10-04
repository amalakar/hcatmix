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

package org.apache.hcatalog.hcatmix.performance;

import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hcatalog.hcatmix.results.LoadStoreScriptRunner;
import org.apache.thrift.TException;
import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Random;

/**
 * Mock class which doesn't do anything, it could be used for testing graph/html generation classes.
 */
public class MockLoadStoreScriptRunner extends LoadStoreScriptRunner {
    private static final Logger LOG = LoggerFactory.getLogger(MockLoadStoreScriptRunner.class);
    private Random random = new Random();
    public MockLoadStoreScriptRunner(String hcatTableSpecFile) throws MetaException, IOException, SAXException,
            ParserConfigurationException, NoSuchObjectException, TException, InvalidObjectException {
        super(hcatTableSpecFile);
    }

    @Override
    public void setUp() throws IOException, TException, NoSuchObjectException, MetaException, SAXException,
            InvalidObjectException, ParserConfigurationException {
    }

    @Override
    protected void runScript(String scriptName) {
        int sleepTime = random.nextInt(1000);
        LOG.info(MessageFormat.format("Supposed to run {0}, but mock runner will only sleep for {1} milliseconds", scriptName, sleepTime));
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void deleteHCatTables() throws NoSuchObjectException, MetaException, TException {
    }

    @Override
    public void deletePigData() {
    }

    @Override
    public void deleteGeneratedDataDir() {
    }

    @Override
    public GroupedTimingStatistics getTimedStats() {
        return super.getTimedStats();
    }
}
