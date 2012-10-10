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

package org.apache.hcatalog.hcatmix.load.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.hcatmix.HCatMixSetup;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;
import org.apache.hcatalog.hcatmix.load.tasks.HCatAddPartitionTask;
import org.apache.hcatalog.hcatmix.load.tasks.HCatListPartitionTask;
import org.apache.hcatalog.hcatmix.load.HadoopLoadGenerator;
import org.apache.hcatalog.hcatmix.load.hadoop.ReduceResult;
import org.apache.hcatalog.hcatmix.loadstore.LoadStoreScriptRunner;
import org.apache.hcatalog.hcatmix.publisher.LoadTestResultsPublisher;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URL;
import java.util.SortedMap;

public class LoadTestRunner extends Configured implements Tool {
    private HCatMixSetup hCatMixSetup;
    private HiveTableSchema tableSchema;
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestRunner.class);
    private final String HCAT_SPEC_FILE = "load_test_table.xml";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new LoadTestRunner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        setUp();
        testAddPartitionTask();
        tearDown();
        return 0;
    }

    public void testListPartitionTask() throws Exception, MetaException {
        HadoopLoadGenerator loadGenerator = new HadoopLoadGenerator();
        SortedMap<Long, ReduceResult> results =  loadGenerator.run(HCatListPartitionTask.class.getName(), getConf());
        LoadTestResultsPublisher publisher = new LoadTestResultsPublisher(results);
        publisher.publishAll();
    }

    public void testAddPartitionTask() throws Exception, MetaException {
        HadoopLoadGenerator loadGenerator = new HadoopLoadGenerator();
        SortedMap<Long, ReduceResult> results =  loadGenerator.run(HCatAddPartitionTask.class.getName(), getConf());
        LoadTestResultsPublisher publisher = new LoadTestResultsPublisher(results);
        publisher.publishAll();
    }

    public void setUp() throws MetaException, IOException, TException, NoSuchObjectException, SAXException, InvalidObjectException, ParserConfigurationException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(HCAT_SPEC_FILE);
        if(url == null) {
            LOG.error(HCAT_SPEC_FILE + " not found");
            throw new RuntimeException(HCAT_SPEC_FILE + " not found");
        }

        String hcatTableSpecFile = url.getPath();

        LoadStoreScriptRunner loadStoreScriptRunner = new LoadStoreScriptRunner(hcatTableSpecFile);

        loadStoreScriptRunner.setUp();
        loadStoreScriptRunner.runPigLoadHCatStoreScript();
        tableSchema = loadStoreScriptRunner.getHiveTableSchema();
    }

    public void tearDown() throws NoSuchObjectException, MetaException, TException {
        hCatMixSetup.deleteTable(tableSchema.getDatabaseName(), tableSchema.getName());
    }

    private void usage() {
        System.out.println("TODO");
    }
}
