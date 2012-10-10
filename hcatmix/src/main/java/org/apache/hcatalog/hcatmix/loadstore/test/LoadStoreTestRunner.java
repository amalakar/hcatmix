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

package org.apache.hcatalog.hcatmix.loadstore.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.hcatmix.HCatMixUtils;
import org.apache.hcatalog.hcatmix.loadstore.LoadStoreScriptRunner;
import org.apache.hcatalog.hcatmix.loadstore.LoadStoreTestResults;
import org.apache.thrift.TException;
import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author: malakar
 */
public class LoadStoreTestRunner extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(LoadStoreTestRunner.class);
    private static LoadStoreTestResults loadStoreTestResults = new LoadStoreTestResults();

    // Use -DhcatSpecFile=<fileName1>,<fileName2> to run load/store for these table specification file only
    private static final String HCAT_SPEC_FILES = "hcatSpecFiles";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new LoadStoreTestRunner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        List<String> specFiles = hcatSpecFileNames();
        for (String specFile : specFiles) {
            testAllLoadStoreScripts(specFile);
        }
        publishResults();
        return 0;
    }

    public List<String> hcatSpecFileNames() {
        final String hcatSpecFile = System.getProperty(HCAT_SPEC_FILES);
        final List<String> specFiles = new ArrayList<String>();

        if (hcatSpecFile == null) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            final String hcatTableSpecFileDir = classLoader.getResource("performance").getPath();
            LOG.info("Will look in directory:" + hcatTableSpecFileDir + " for hcatalog table specification files");
            File hcatSpecDir = new File(hcatTableSpecFileDir);

            if(hcatSpecDir.listFiles() == null) {
                HCatMixUtils.logAndThrow(new RuntimeException("The list of hcatSpec files is null"));
            }
            FilenameFilter xmlFilter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".xml");
                }
            };
            for (File file : hcatSpecDir.listFiles(xmlFilter)) {
                specFiles.add(file.getAbsolutePath());
            }
        } else {
            LOG.info(MessageFormat.format("Honouring command line option: -D{0}={1}", HCAT_SPEC_FILES, hcatSpecFile));
            specFiles.addAll(Arrays.asList(hcatSpecFile.split(",")));
        }
        return specFiles;
    }


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

    public static void publishResults() throws Exception {
        loadStoreTestResults.publish();
    }
}
