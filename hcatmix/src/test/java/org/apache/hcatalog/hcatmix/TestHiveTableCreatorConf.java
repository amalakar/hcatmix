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

package org.apache.hcatalog.hcatmix;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestHiveTableCreatorConf {

    @Test
    public void testBuilder() {
        try {
            new HiveTableCreatorConf.Builder().generateData().build();
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Output directory cannot be null/empty, when data is to be generated", e.getMessage());
        }

        try {
            new HiveTableCreatorConf.Builder().outputDir("/tmp/test").build();
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Pig script output directory cannot be null/empty, when pig script is to be generated", e.getMessage());
        }

        HiveTableCreatorConf conf = new HiveTableCreatorConf.Builder().outputDir("/tmp/data").pigScriptDir("/tmp/pig").build();
        assertNotNull(conf);
        assertEquals("/tmp/data/", conf.getOutputDir());
        assertEquals("/tmp/pig/", conf.getPigScriptDir());
        assertEquals(true, conf.isGeneratePigScripts());
        assertEquals(true, conf.isCreateTable());
        assertEquals(true, conf.isGenerateData());

        // Check that '/' is not appended if not required
        conf = new HiveTableCreatorConf.Builder().outputDir("/tmp/data/").pigScriptDir("/tmp/pig/").build();
        assertNotNull(conf);
        assertEquals("/tmp/data/", conf.getOutputDir());
        assertEquals("/tmp/pig/", conf.getPigScriptDir());

        try {
            new HiveTableCreatorConf.Builder().outputDir("/tmp/test").pigScriptDir("/tmp/pig").doEverything().generateData().build();
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Special switch for creating table, generating data and for generating pig scripts cannot be set when do-everything is set", e.getMessage());
        }

    }
}
