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

package org.apache.hcatalog.hcatmix.conf;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.pig.test.utils.DataType;
import org.apache.pig.test.utils.datagen.ColSpec;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestTableSchemaXMLParser {

    @Test
    public void testXMLConfFile() throws IOException, SAXException, ParserConfigurationException {
        String file = getClass().getClassLoader().getResource("hcat_table_conf_test.xml").getPath();
        TableSchemaXMLParser configParser = new TableSchemaXMLParser(file);
        List<HiveTableSchema> multiInstanceList = configParser.getHiveTableList();
        assertEquals(4, multiInstanceList.size());

        HiveTableSchema table = multiInstanceList.get(0);
        assertEquals("page_views_199_0", table.getName());
        assertEquals("default", table.getDatabaseName());

        // Check columns of table1
        List<ColSpec> cols = table.getColumnColSpecs();
        assertEquals(2, cols.size());
        ColSpec columnColSpec = cols.get(0);
        assertEquals(DataType.STRING, columnColSpec.getDataType());
        assertEquals(5, columnColSpec.getAvgStrLength());
        assertEquals(ColSpec.DistributionType.UNIFORM, columnColSpec.getDistype());
        assertEquals(6, columnColSpec.getCardinality());
        assertEquals(7, columnColSpec.getPercentageNull());

        List<FieldSchema> columnFieldSchemas = table.getColumnFieldSchemas();
        assertEquals(2, columnFieldSchemas.size());
        FieldSchema colFieldSchema = columnFieldSchemas.get(0);
        assertEquals("user", colFieldSchema.getName());
        assertEquals("string", colFieldSchema.getType());

        columnColSpec = cols.get(1);
        assertEquals(DataType.INT, columnColSpec.getDataType());
        assertEquals(ColSpec.DistributionType.ZIPF, columnColSpec.getDistype());
        assertEquals(8, columnColSpec.getCardinality());
        assertEquals(9, columnColSpec.getPercentageNull());

        colFieldSchema = columnFieldSchemas.get(1);
        assertEquals("timespent", colFieldSchema.getName());
        assertEquals("int", colFieldSchema.getType());

        // Check partitions of table1
        List<ColSpec> partitionCols = table.getPartitionColSpecs();
        assertEquals(2, partitionCols.size());
        ColSpec partition = partitionCols.get(0);
        assertEquals(DataType.STRING, partition.getDataType());
        assertEquals(99, partition.getAvgStrLength());
        assertEquals(ColSpec.DistributionType.UNIFORM, partition.getDistype());
        assertEquals(21, partition.getCardinality());

        List<FieldSchema> partitionFields = table.getPartitionFieldSchemas();
        FieldSchema partitionFieldSchema = partitionFields.get(0);
        assertEquals("action", partitionFieldSchema.getName());
        assertEquals("string", partitionFieldSchema.getType());

        partition = partitionCols.get(1);
        assertEquals(DataType.INT, partition.getDataType());
        assertEquals(ColSpec.DistributionType.ZIPF, partition.getDistype());
        assertEquals(29, partition.getCardinality());

        partitionFieldSchema = partitionFields.get(1);
        assertEquals("age", partitionFieldSchema.getName());
        assertEquals("int", partitionFieldSchema.getType());

        assertEquals(199, table.getRowCount());

        // Following are the tables with same spec but multiple instances
        // Check for table2
        table = multiInstanceList.get(1);
        assertEquals("users_50_0", table.getName());
        assertEquals("db1", table.getDatabaseName());
        verifyTable(table);

        // Check for table3
        table = multiInstanceList.get(2);
        assertEquals("users_99_0", table.getName());
        assertEquals("db1", table.getDatabaseName());
        verifyTable(table);

        // Check for table4
        table = multiInstanceList.get(3);
        assertEquals("users_99_1", table.getName());
        assertEquals("db1", table.getDatabaseName());
        verifyTable(table);
    }

    private void verifyTable(HiveTableSchema table) {
        // Check columns of table
        List<ColSpec> cols = table.getColumnColSpecs();
        assertEquals(1, cols.size());
        ColSpec col = cols.get(0);
        assertEquals(DataType.STRING, col.getDataType());
        assertEquals(11, col.getAvgStrLength());
        assertEquals(ColSpec.DistributionType.UNIFORM, col.getDistype());
        assertEquals(12, col.getCardinality());
        assertEquals(13, col.getPercentageNull());

        List<FieldSchema> columnFieldSchemas = table.getColumnFieldSchemas();
        assertEquals(1, columnFieldSchemas.size());
        FieldSchema colFieldSchema = columnFieldSchemas.get(0);
        assertEquals("uid", colFieldSchema.getName());
        assertEquals("string", colFieldSchema.getType());
    }
}
