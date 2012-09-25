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

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hcatalog.hcatmix.HCatMixSetup;
import org.apache.hcatalog.hcatmix.HCatMixSetupConf;
import org.apache.hcatalog.hcatmix.HCatMixUtils;
import org.apache.pig.PigServer;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

@BenchmarkMethodChart(filePrefix = "benchmark-lists")
public class RunLoadScripts {
    public static PigServer pigServer;
    private static final Logger LOG = LoggerFactory.getLogger(RunLoadScripts.class);
    private static final String HCATMIX_LOCAL_ROOT = "/tmp/hcatmix";
    private static final String HCATMIX_HDFS_ROOT = "/tmp/hcatmix";
    private static final String HCATMIX_PIG_SCRIPT_DIR = HCATMIX_LOCAL_ROOT + "/pig_scripts";
    private static HCatMixSetup hCatMixSetup;
    public MethodRule benchmarkRun = new BenchmarkRule();

    public final String TABLE_NAME = "page_views_199_0";
    public final String DB_NAME = "default";

    @AxisRange(min = 0, max = 10000)

    @BeforeClass
    public static void prepare() throws IOException, MetaException {
        pigServer = new PigServer("mapreduce");
        String hcatDir = System.getenv("HCAT_HOME");
        File hcatLibDir = new File(hcatDir + "/lib/");
        for (File jarFile : hcatLibDir.listFiles()) {
            pigServer.registerJar(jarFile.getAbsolutePath());
        }
    }

    @BeforeClass
    public static void setUp() throws MetaException, IOException, SAXException, ParserConfigurationException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String hcatConfFile = classLoader.getResource("hcat_table_specification.xml").getPath();
        HCatMixSetupConf conf = new HCatMixSetupConf.Builder().confFileName(hcatConfFile).outputDir(HCATMIX_HDFS_ROOT + "/data")
                .numMappers(2).pigScriptDir(HCATMIX_PIG_SCRIPT_DIR).pigDataOutputDir(HCATMIX_HDFS_ROOT + "/pigdata").build();
        hCatMixSetup = new HCatMixSetup();
        hCatMixSetup.setupFromConf(conf);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void testHCatStore() throws IOException {
        LOG.info("Running pig script using pig load/HCat store");
        pigServer.registerScript(HCatMixUtils.getHCatStoreScriptName(HCATMIX_PIG_SCRIPT_DIR, TABLE_NAME));
        LOG.info("Successfully ran pig script: pig load/HCat store");
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void testHCatLoad() throws IOException {
        LOG.info("Running pig script using HCat load/pig store");
        pigServer.registerScript(HCatMixUtils.getHCatLoadScriptName(HCATMIX_PIG_SCRIPT_DIR, TABLE_NAME));
        LOG.info("Successfully ran pig script: HCat load/pig store");
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void testPigLoadStore() throws IOException {
        LOG.info("Running pig script using pig load/pig store");
        pigServer.registerScript(HCatMixUtils.getPigLoadStoreScriptName(HCATMIX_PIG_SCRIPT_DIR, TABLE_NAME));
        LOG.info("Successfully ran pig script: pig load/pig store");
    }

    @After
    public void tearDown() throws NoSuchObjectException, MetaException, TException {
        LOG.info("TearDown: Will delete table: " + TABLE_NAME + " and delete directory: " + HCATMIX_HDFS_ROOT + "/pigdata" );
        hCatMixSetup.deleteTable(DB_NAME, TABLE_NAME);
        File pigData = new File(HCATMIX_HDFS_ROOT + "/pigdata");
        try {
            FileUtil.fullyDelete(pigData);
        } catch (IOException e) {
            LOG.error("Could not delete directory: " + pigData.getAbsolutePath());
        }
    }

    @AfterClass
    public static void deleteDataDir() {
        File data = new File(HCATMIX_HDFS_ROOT + "/data");
        try {
            FileUtil.fullyDelete(data);
        } catch (IOException e) {
            LOG.error("Could not delete directory: " + data.getAbsolutePath());
        }
    }
}
