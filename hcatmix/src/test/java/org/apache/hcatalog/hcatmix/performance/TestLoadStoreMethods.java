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

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hcatalog.hcatmix.HCatMixSetup;
import org.apache.hcatalog.hcatmix.HCatMixSetupConf;
import org.apache.hcatalog.hcatmix.HCatMixUtils;
import org.apache.hcatalog.hcatmix.PigScriptGenerator;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;
import org.apache.hcatalog.hcatmix.conf.TableSchemaXMLParser;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;

@BenchmarkMethodChart(filePrefix = "benchmark-lists")
public abstract class TestLoadStoreMethods {
    private static final Logger LOG = LoggerFactory.getLogger(TestLoadStoreMethods.class);
    private static final String HCATMIX_LOCAL_ROOT = "/tmp/hcatmix";
    private static final String HCATMIX_HDFS_ROOT = "/tmp/hcatmix";
    private static final String HCATMIX_PIG_SCRIPT_DIR = HCATMIX_LOCAL_ROOT + "/pig_scripts";
    private static HCatMixSetup hCatMixSetup;
    public MethodRule benchmarkRun = new BenchmarkRule();

    private static String tableName;
    public static String dbName;
    public static final int NUM_MAPPERS = 1;
    public static String additionalJars;

    protected static String hcatTableSpecFileName;

    @AxisRange(min=0, max=Double.MAX_VALUE)

    @BeforeClass
    public static void setUp() throws MetaException, IOException, SAXException, ParserConfigurationException, NoSuchObjectException, TException, InvalidObjectException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final String hcatTableSpecFile = classLoader.getResource(hcatTableSpecFileName).getPath();

        HCatMixSetupConf conf = new HCatMixSetupConf.Builder().confFileName(hcatTableSpecFile)
                .createTable().pigScriptDir(HCATMIX_PIG_SCRIPT_DIR).pigDataOutputDir(HCATMIX_HDFS_ROOT + "/pigdata").build();
        hCatMixSetup = new HCatMixSetup();
        hCatMixSetup.setupFromConf(conf);

        String hcatDir = System.getenv("HCAT_HOME");
        File hcatLibDir = new File(hcatDir + "/lib/");

        StringBuffer jars = new StringBuffer();
        String delim = "";
        for (File jarFile : hcatLibDir.listFiles()) {
            jars.append(delim).append(jarFile);
            delim = ":";
        }
        additionalJars = jars.toString();

        TableSchemaXMLParser configParser = new TableSchemaXMLParser(hcatTableSpecFile);
        List<HiveTableSchema> multiInstanceList = configParser.getHiveTableList();
        assertEquals(1, multiInstanceList.size());
        HiveTableSchema hiveTableSchema = multiInstanceList.get(0);
        tableName = hiveTableSchema.getName();
        dbName = hiveTableSchema.getDatabaseName();

        // Create a table for testing copying from one HCat table to another
        hiveTableSchema.setName(HCatMixUtils.getCopyTableName(tableName));
        try {
            LOG.info("About to create table: " + hiveTableSchema.getName());
            hCatMixSetup.createTable(hiveTableSchema);
            LOG.info("Successfully created table: " + hiveTableSchema.getName());
        } catch (AlreadyExistsException e) {
            LOG.info("Couldn't create table, " + hiveTableSchema.getName() + ". Already exists ignored and proceeding", e);
        }
    }

    public void runScript(String scriptName) {
        PigProgressListener listener = new PigProgressListener();
        final String logFileName =  System.getenv("buildDirectory") + "/" + getClass() + scriptName + ".log";
        String[] args = {"-Dpig.additional.jars=" + additionalJars, "-f", scriptName, "-l", logFileName};
        PigRunner.run(args, listener);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void testPigLoadHCatStore() throws IOException {
        LOG.info("Running pig script using pig load/HCat store");
        runScript(HCatMixUtils.getHCatStoreScriptName(HCATMIX_PIG_SCRIPT_DIR, tableName));
        LOG.info("Successfully ran pig script: pig load/HCat store");
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void testHCatLoadPigStore() throws IOException {
        LOG.info("Running pig script using HCat load/pig store");
        runScript(HCatMixUtils.getHCatLoadScriptName(HCATMIX_PIG_SCRIPT_DIR, tableName));
        LOG.info("Successfully ran pig script: HCat load/pig store");
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void testPigLoadPigStore() throws IOException {
        LOG.info("Running pig script using pig load/pig store");
        runScript(HCatMixUtils.getPigLoadStoreScriptName(HCATMIX_PIG_SCRIPT_DIR, tableName));
        LOG.info("Successfully ran pig script: pig load/pig store");
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void testHCatLoadHCatStore() throws IOException {
        LOG.info("Running pig script using hcat load/ store");
        runScript(HCatMixUtils.getPigLoadStoreScriptName(HCATMIX_PIG_SCRIPT_DIR, tableName));
        LOG.info("Successfully ran pig script: pig load/pig store");
    }

//    @After
//    public void tearDown() throws NoSuchObjectException, MetaException, TException {
//        LOG.info("TearDown: Will delete table: " + tableName + " and delete directory: " + HCATMIX_HDFS_ROOT + "/pigdata" );
//        hCatMixSetup.deleteTable(dbName, tableName);
//        File pigData = new File(HCATMIX_HDFS_ROOT + "/pigdata");
//        try {
//            FileUtil.fullyDelete(pigData);
//        } catch (IOException e) {
//            LOG.error("Could not delete directory: " + pigData.getAbsolutePath());
//        }
//    }
//
//    @AfterClass
//    public static void deleteDataDir() {
//        File data = new File(HCATMIX_HDFS_ROOT + "/data");
//        try {
//            FileUtil.fullyDelete(data);
//        } catch (IOException e) {
//            LOG.error("Could not delete directory: " + data.getAbsolutePath());
//        }
//    }

    public static class PigProgressListener implements PigProgressNotificationListener {

        @Override
        public void launchStartedNotification(String scriptId, int numJobsToLaunch) {
            LOG.info(scriptId + " numJob: " + numJobsToLaunch);
        }

        @Override
        public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) {
        }

        @Override
        public void jobStartedNotification(String scriptId, String assignedJobId) {
        }

        @Override
        public void jobFinishedNotification(String scriptId, JobStats jobStats) {
            LOG.info(scriptId + "Finished Job: " + jobStats.toString());
        }

        @Override
        public void jobFailedNotification(String scriptId, JobStats jobStats) {
        }

        @Override
        public void outputCompletedNotification(String scriptId, OutputStats outputStats) {
        }

        @Override
        public void progressUpdatedNotification(String scriptId, int progress) {
        }

        @Override
        public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
        }
    }
}
