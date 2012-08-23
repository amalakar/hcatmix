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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchemas;
import org.apache.hcatalog.hcatmix.conf.TableSchemaXMLParser;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

public class HiveTableCreator {
    HiveMetaStoreClient hiveClient;

    public static void main(String[] args) {
        try {
            testHiveTableConf(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testHiveTableConf(final String fileName) throws IOException, SAXException, ParserConfigurationException, MetaException {
        TableSchemaXMLParser configParser = new TableSchemaXMLParser(fileName);
        HiveTableSchemas schemas = configParser.getHiveTableSchemas();
        HiveTableCreator tableCreator = new HiveTableCreator();
        tableCreator.createTables(schemas);
    }

    public HiveTableCreator() throws MetaException {
        HiveConf hiveConf = new HiveConf(HiveTableCreator.class);
        hiveClient = new HiveMetaStoreClient(hiveConf);
    }

    public void createTables(HiveTableSchemas tableSchemas) {
        for (HiveTableSchema hiveTableSchema : tableSchemas) {
            createTable(hiveTableSchema);
        }
    }

    public void createTable(HiveTableSchema hiveTableSchema) {
        Table table = new Table();
        table.setDbName(hiveTableSchema.getDatabaseName());
        table.setTableName(hiveTableSchema.getName());
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(hiveTableSchema.getColumns());
        table.setSd(sd);
        sd.setParameters(new HashMap<String, String>());
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(table.getTableName());
        sd.getSerdeInfo().setParameters(new HashMap<String, String>());

        sd.setInputFormat(org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName());
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.RCFileOutputFormat.class.getName());
        sd.getSerdeInfo().getParameters().put(
                org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
        sd.getSerdeInfo().setSerializationLib(
                org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
        table.setPartitionKeys(hiveTableSchema.getPartitions());

        try {
            System.out.println(table.getTableName());
            hiveClient.createTable(table);
        } catch (Exception e) {
            System.out.println("Error is because:" + e);
            e.printStackTrace();
        } finally {
            System.out.println("finally");
        }
    }
}