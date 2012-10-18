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
import org.apache.hcatalog.hcatmix.loadstore.LoadStoreScriptRunner;
import org.apache.hcatalog.hcatmix.loadstore.LoadStoreTestResults;
import org.apache.hcatalog.hcatmix.performance.conf.LoadStoreTestConf;
import org.apache.hcatalog.hcatmix.performance.conf.LoadStoreTestsConf;
import org.apache.thrift.TException;
import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.*;

import static org.testng.Assert.assertNotNull;

public class TestLoadStoreScripts {
    private static final Logger LOG = LoggerFactory.getLogger(TestLoadStoreScripts.class);
    private static final LoadStoreTestResults loadStoreTestResults = new LoadStoreTestResults();
    private static final String LOAD_STORE_TESTS_CONF = "hcatmix_load_store_tests.yml";

    // Use -DhcatSpecFile=<fileName> to runLoadTest load/store for these table specification file only
    private static final String HCAT_SPEC_FILE_ARG_NAME = "hcatSpecFile";
    private static final String HCAT_NUM_RUN_ARG_NAME = "numRun";
    private static final String HCAT_DATAGEN_NUM_MAPPERS_ARG_NAME = "numDataGenMappers";

    /**
     * Load the yaml file and provide the individual configuration to the test method. Individual tests can be ran
     * using -DhcatSpecFile=<fileName> -DnumRun=2 -DnumDataGenMappers=3 cmd line arguments
     * @return
     */
    @DataProvider(name = "LoadStoreTests")
    public Iterator<Object[]> loadStoreConfProvider() {
        final String hcatSpecFile = System.getProperty(HCAT_SPEC_FILE_ARG_NAME);
        final List<Object[]> testArgs = new ArrayList<Object[]>();

        if (hcatSpecFile == null) {
            // yaml magic to load the config file
            Constructor constructor = new Constructor(LoadStoreTestsConf.class);
            TypeDescription testDescription = new TypeDescription(LoadStoreTestsConf.class);
            testDescription.putListPropertyType("tests", LoadStoreTestConf.class);
            constructor.addTypeDescription(testDescription);
            Yaml yaml = new Yaml(constructor);
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream conf = classLoader.getResourceAsStream(LOAD_STORE_TESTS_CONF);
            LoadStoreTestsConf loadStoreTestsConf = (LoadStoreTestsConf) yaml.load(conf);

            for (LoadStoreTestConf loadStoreTestConf : loadStoreTestsConf.getTests()) {
                Object[] argument = {loadStoreTestConf.getFileName(),
                        loadStoreTestConf.getNumRuns(), loadStoreTestConf.getNumDataGenMappers()};
                testArgs.add(argument);
            }

        } else {
            LOG.info(MessageFormat.format("Honouring command line option: -D{0}={1}", HCAT_SPEC_FILE_ARG_NAME, hcatSpecFile));
            int numDataGenMappers = Integer.parseInt(System.getProperty(HCAT_DATAGEN_NUM_MAPPERS_ARG_NAME, "10"));
            int numRuns = Integer.parseInt(System.getProperty(HCAT_NUM_RUN_ARG_NAME, "10"));
            Object[] argument = {hcatSpecFile, numRuns, numDataGenMappers};
            testArgs.add(argument);
        }
        return testArgs.iterator();
    }


    @Test(dataProvider = "LoadStoreTests")
    public void testAllLoadStoreScripts(String hcatSpecFileName, int numRuns, int numDataGenMappers) throws IOException, TException, NoSuchObjectException,
            MetaException, SAXException, InvalidObjectException, ParserConfigurationException {
        LOG.info(MessageFormat.format("HCatalog spec file name: {0}, number of runs: {1}, number of mapper for data generation {3}",
                hcatSpecFileName, numRuns, numDataGenMappers));
        LoadStoreScriptRunner runner = new LoadStoreScriptRunner(hcatSpecFileName, numDataGenMappers);

        for (int i = 0; i < numRuns; i++) {
            LOG.info(MessageFormat.format("{0}: Run - {1}/{2}", hcatSpecFileName, i, numRuns));
            try {
                runner.setUp();
                runner.runPigLoadHCatStoreScript();
                runner.runHCatLoadPigStoreScript();
                runner.runPigLoadPigStoreScript();
                runner.runHCatLoadHCatStoreScript();

                runner.deleteHCatTables();
                runner.deletePigData();
            } catch (IOException e) {
                LOG.error("Error running script: " + hcatSpecFileName + " ignored.", e);
            }
        }

        GroupedTimingStatistics stats = runner.getTimedStats();
        loadStoreTestResults.addResult(hcatSpecFileName, stats);
    }

    @AfterClass
    public static void publishResults() throws Exception {
        loadStoreTestResults.publish();
    }

    public static class LoadStoreScriptConfig {
        public String hcatSpecFileName;
        public int numRuns;

        public LoadStoreScriptConfig(String hcatSpecFileName, int numRuns) {
            this.hcatSpecFileName = hcatSpecFileName;
            this.numRuns = numRuns;
        }
    }
}
