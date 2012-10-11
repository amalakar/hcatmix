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
import org.apache.hcatalog.hcatmix.load.HadoopLoadGenerator;
import org.apache.hcatalog.hcatmix.load.hadoop.ReduceResult;
import org.apache.hcatalog.hcatmix.loadstore.LoadStoreScriptRunner;
import org.apache.hcatalog.hcatmix.publisher.LoadTestResultsPublisher;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.SortedMap;

public class LoadTestRunner extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestRunner.class);
    private final String LOAD_TEST_HCAT_SPEC_FILE = "load_test_table.xml";
    private LoadStoreScriptRunner loadStoreScriptRunner;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new LoadTestRunner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('c', "confFile", CmdLineParser.ValueExpected.REQUIRED);

        char opt;
        setUp();
        parseOption: try {
            while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
                switch (opt) {
                    case 'c':
                        runTest(opts.getValStr());
                        break parseOption;
                    default:
                        LOG.error("Unrecognized option " + opts.getValStr());
                        usage();
                }
            }
        } catch (ParseException pe) {
            System.err.println("Couldn't parse the command line arguments, " +
                    pe.getMessage());
            usage();
        }

        tearDown();
        return 0;
    }

    public void runTest(String confFile) throws Exception {
        HadoopLoadGenerator loadGenerator = new HadoopLoadGenerator();
        SortedMap<Long, ReduceResult> results =  loadGenerator.run(confFile, getConf());
        LoadTestResultsPublisher publisher = new LoadTestResultsPublisher(results);
        publisher.publishAll();
    }

    public void setUp() throws MetaException, IOException, TException, NoSuchObjectException, SAXException, InvalidObjectException, ParserConfigurationException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(LOAD_TEST_HCAT_SPEC_FILE);
        if(url == null) {
            LOG.error(LOAD_TEST_HCAT_SPEC_FILE + " not found");
            throw new RuntimeException(LOAD_TEST_HCAT_SPEC_FILE + " not found");
        }

        String hcatTableSpecFile = url.getPath();

        loadStoreScriptRunner = new LoadStoreScriptRunner(hcatTableSpecFile);
        loadStoreScriptRunner.setUp();
        loadStoreScriptRunner.runPigLoadHCatStoreScript();
    }

    public void tearDown() throws NoSuchObjectException, MetaException, TException {
        loadStoreScriptRunner.deleteHCatTables();
    }

    private void usage() {
        System.out.println("TODO");
    }
}
