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
import org.apache.pig.PigServer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class RunLoadScripts extends AbstractBenchmark {
    public static PigServer pigServer;
    private static final Logger LOG = LoggerFactory.getLogger(RunLoadScripts.class);

//    private static URL hiveSiteURL = null;
//        static {
//        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//        if (classLoader == null) {
//            classLoader = HiveConf.class.getClassLoader();
//        }
//
//        // Log a warning if hive-default.xml is found on the classpath
//        URL hiveDefaultURL = classLoader.getResource("hive-default.xml");
//        if (hiveDefaultURL != null) {
//            l4j.warn("DEPRECATED: Ignoring hive-default.xml found on the CLASSPATH at " +
//                hiveDefaultURL.getPath());
//        }
//
//        // Look for hive-site.xml on the CLASSPATH and log its location if found.
//        hiveSiteURL = classLoader.getResource("hive-site.xml");
//        if (hiveSiteURL == null) {
//            l4j.warn("ARUP::: hive-site.xml not found on CLASSPATH");
//        } else {
//            l4j.debug("ARUP::: Using hive-site.xml found on CLASSPATH at " + hiveSiteURL.getPath());
//        }
//
//    }
    @BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 0)

    @BeforeClass
    public static void prepare() throws IOException {
        pigServer = new PigServer("mapreduce");
        String hcatDir = System.getenv("HCAT_HOME");
        File hcatLibDir = new File(hcatDir + "/lib/");
        for (File jarFile : hcatLibDir.listFiles()) {
            pigServer.registerJar(jarFile.getAbsolutePath());
        }
    }

    @Test
    public void testLoadScripts() throws IOException {
        pigServer.registerScript("/tmp/pig/page_views_199_0.load.pig");
        // pigServer.
    }
}
