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

import org.apache.hcatalog.hcatmix.conf.DefaultHiveTableSchema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.pig.test.utils.DataType;
import org.apache.pig.test.utils.datagen.ColSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class TestPigScriptGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TestPigScriptGenerator.class);

    private static final DefaultHiveTableSchema tableSchema = new DefaultHiveTableSchema();

    @BeforeClass
    public static void setUp() {
        tableSchema.setName("my_table");
        tableSchema.setDatabaseName("default_db");

        List<ColSpec> columnColSpecList = new ArrayList<ColSpec>();
        columnColSpecList.add(new ColSpec.Builder().dataType(DataType.STRING).avgStrLength(20).cardinality(160000)
            .distributionType(ColSpec.DistributionType.ZIPF).percentageNull(10).build());

        columnColSpecList.add(new ColSpec.Builder().dataType(DataType.INT).cardinality(200)
            .distributionType(ColSpec.DistributionType.UNIFORM).build());

        tableSchema.setColumnColSpecs(columnColSpecList);

        List<FieldSchema> columnFieldSchemaList = new ArrayList<FieldSchema>();
        columnFieldSchemaList.add(new FieldSchema("uri", DataType.STRING.toString().toLowerCase(), ""));
        columnFieldSchemaList.add(new FieldSchema("ip", DataType.INT.toString().toLowerCase(), ""));
        tableSchema.setColumnFieldSchemas(columnFieldSchemaList);

        List<ColSpec> partitionColSpecList = new ArrayList<ColSpec>();
        partitionColSpecList.add(new ColSpec.Builder().dataType(DataType.STRING).avgStrLength(20).cardinality(100)
            .distributionType(ColSpec.DistributionType.ZIPF).percentageNull(10).build());

        partitionColSpecList.add(new ColSpec.Builder().dataType(DataType.DOUBLE).cardinality(200)
            .distributionType(ColSpec.DistributionType.UNIFORM).build());

        tableSchema.setPartitionColSpecs(partitionColSpecList);
        List<FieldSchema> partitionFieldSchemaList = new ArrayList<FieldSchema>();
        partitionFieldSchemaList.add(new FieldSchema("uri", DataType.STRING.toString().toLowerCase(), ""));
        partitionFieldSchemaList.add(new FieldSchema("ip", DataType.INT.toString().toLowerCase(), ""));
        tableSchema.setPartitionFieldSchemas(partitionFieldSchemaList);
    }

    @Test
    public void testPigLoaderHCatStorer() {
        PigScriptGenerator pigScriptGenerator = new PigScriptGenerator("/tmp/table", "/tmp/pig_dir", tableSchema);
        final String EXPECTED = "input_data = load '/tmp/table' USING PigStorage(',') AS (uri:chararray, ip:int, uri:chararray, ip:int);\n" +
            "STORE input_data into 'default_db.my_table' USING  org.apache.hcatalog.pig.HCatStorer();\n";
        final String pigScript = pigScriptGenerator.getPigLoaderHCatStorerScript();
        LOG.info("Pig loader/HCAT storer script: \n" + pigScript);
        Assert.assertEquals(EXPECTED, pigScript);
    }

    @Test
    public void testPigLoaderPigStorer() {
        PigScriptGenerator pigScriptGenerator = new PigScriptGenerator("/tmp/table", "/tmp/pig_dir", tableSchema);
        final String EXPECTED = "input_data = load '/tmp/table' USING PigStorage(',') AS (uri:chararray, ip:int, uri:chararray, ip:int);\n" +
            "STORE input_data into '/tmp/pig_dir/default_db.my_table/pig_load_pig_store' USING  PigStorage();\n";
        final String pigScript = pigScriptGenerator.getPigLoaderPigStorerScript();
        LOG.info("Pig loader/pig storer script: \n" + pigScript);
        Assert.assertEquals(EXPECTED, pigScript);
    }

    @Test
    public void testHCatLoaderPigStorer() {
        PigScriptGenerator pigScriptGenerator = new PigScriptGenerator("/tmp/table", "/tmp/pig_dir", tableSchema);
        final String EXPECTED = "input_data = load 'default_db.my_table' USING org.apache.hcatalog.pig.HCatLoader();\n" +
            "STORE input_data into '/tmp/pig_dir/default_db.my_table/hcat_load_pig_store' USING  PigStorage();\n";
        final String pigScript = pigScriptGenerator.getHCatLoaderPigStorerScript();
        LOG.info("HCat loader/pig storer script: \n" + pigScript);
        Assert.assertEquals(EXPECTED, pigScript);
    }

    @Test
    public void testHCatLoaderHCatStorer() {
        PigScriptGenerator pigScriptGenerator = new PigScriptGenerator("/tmp/table", "/tmp/pig_dir", tableSchema);
        final String EXPECTED = "input_data = load 'default_db.my_table' USING org.apache.hcatalog.pig.HCatLoader();\n" +
                "STORE input_data into 'default_db.my_table_copy' USING  org.apache.hcatalog.pig.HCatStorer();\n";
        final String pigScript = pigScriptGenerator.getHCatLoaderHCatStorerScript();
        LOG.info("HCat loader/HCat storer script: \n" + pigScript);
        Assert.assertEquals(EXPECTED, pigScript);
    }
}
