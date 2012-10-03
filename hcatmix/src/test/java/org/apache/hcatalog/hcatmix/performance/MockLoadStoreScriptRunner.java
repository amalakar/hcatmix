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
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Random;

/**
 * Mock class which doesn't do anything, to test graph/html generation etc.
 */
public class MockLoadStoreScriptRunner extends LoadStoreScriptRunner {
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
        try {
            Thread.sleep(new Random().nextInt(100));
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
        return super.getTimedStats();    //To change body of overridden methods use File | Settings | File Templates.
    }
}
