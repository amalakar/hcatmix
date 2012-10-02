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
import org.apache.hcatalog.hcatmix.HCatGrapher;
import org.apache.hcatalog.hcatmix.HTMLWriter;
import org.apache.thrift.TException;
import org.perf4j.GroupedTimingStatistics;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertNotNull;

public class TestLoadStoreScripts {

    // Use -DhcatSpecFile=<fileName> to run load/store for a single hcat table specification file
    private static final String HCAT_SPEC_FILE = "hcatSpecFile";
    private static List<String> urls = new ArrayList<String>();

    @DataProvider(name = "HCatSpecFileNames")
    public Iterator<Object[]> hcatSpecFileNames() {
        final String hcatSpecFile = System.getProperty(HCAT_SPEC_FILE);
        final List<Object[]> specFiles = new ArrayList<Object[]>();

        if (hcatSpecFile == null) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            final String hcatTableSpecFileDir = classLoader.getResource("performance").getPath();
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
            specFiles.add(new Object[]{ hcatSpecFile});
        }
        return specFiles.iterator();
    }


    @Test(dataProvider = "HCatSpecFileNames")
    public void testAllLoadStoreScripts(String specFileName) throws IOException, TException, NoSuchObjectException, MetaException, SAXException, InvalidObjectException, ParserConfigurationException {
        System.out.println("Spec file name: "  +specFileName);
        LoadStoreScriptRunner runner = new LoadStoreScriptRunner(specFileName);

        int numRuns = 2;
        for (int i = 0; i < numRuns; i++) {
            runner.runPigLoadHCatStoreScript();
            runner.runHCatLoadPigStoreScript();
            runner.runPigLoadPigStoreScript();
            runner.runHCatLoadHCatStoreScript();

            runner.deleteHCatTables();
            runner.deletePigData();
        }

        GroupedTimingStatistics stats = runner.getTimedStats();
        String chartUrl = HCatGrapher.createChart(stats);
        urls.add(chartUrl);
    }

    @AfterClass
    public static void publishResults() throws IOException {
        HTMLWriter.publish(urls);
    }
}
