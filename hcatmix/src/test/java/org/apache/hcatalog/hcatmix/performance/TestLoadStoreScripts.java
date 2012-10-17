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
import org.apache.thrift.TException;
import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;

import static org.testng.Assert.assertNotNull;

public class TestLoadStoreScripts {
    private static final Logger LOG = LoggerFactory.getLogger(TestLoadStoreScripts.class);
    private static LoadStoreTestResults loadStoreTestResults = new LoadStoreTestResults();

    // Use -DhcatSpecFile=<fileName1>,<fileName2> to runLoadTest load/store for these table specification file only
    private static final String HCAT_SPEC_FILES_ARG_NAME = "hcatSpecFiles";

    @DataProvider(name = "HCatSpecFileNames")
    public Iterator<Object[]> hcatSpecFileNames() {
        final String hcatSpecFile = System.getProperty(HCAT_SPEC_FILES_ARG_NAME);
        final List<Object[]> specFiles = new ArrayList<Object[]>();

        if (hcatSpecFile == null) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            final String hcatTableSpecFileDir = classLoader.getResource("performance").getPath();
            LOG.info("Will look in directory:" + hcatTableSpecFileDir + " for hcatalog table specification files");
            File hcatSpecDir = new File(hcatTableSpecFileDir);

            assertNotNull(hcatSpecDir.listFiles());
            FilenameFilter xmlFilter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".xml");
                }
            };
            for (File file : hcatSpecDir.listFiles(xmlFilter)) {
                specFiles.add(new Object[]{ file.getAbsolutePath()});
            }
        } else {
            LOG.info(MessageFormat.format("Honouring command line option: -D{0}={1}", HCAT_SPEC_FILES_ARG_NAME, hcatSpecFile));
            specFiles.add(hcatSpecFile.split(","));
        }
        return specFiles.iterator();
    }


    @Test(dataProvider = "HCatSpecFileNames")
    public void testAllLoadStoreScripts(String hcatSpecFileName) throws IOException, TException, NoSuchObjectException,
            MetaException, SAXException, InvalidObjectException, ParserConfigurationException {
        LOG.info("HCatalog spec file name: " + hcatSpecFileName);
        LoadStoreScriptRunner runner = new LoadStoreScriptRunner(hcatSpecFileName);

        int numRuns = 10;
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
