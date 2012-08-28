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
import org.apache.pig.test.utils.datagen.ColSpec;
import org.apache.pig.test.utils.datagen.DataGenerator;
import org.apache.pig.test.utils.datagen.DataGeneratorConf;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HiveTableCreator {
    HiveMetaStoreClient hiveClient;

    public static void main(String[] args) {
        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('m', "mappers", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('o', "output", CmdLineParser.ValueExpected.REQUIRED);

        DataGeneratorConf.Builder builder = new DataGeneratorConf.Builder();
        String fileName = null;
        int numMappers = 0;
        String outputDir = null;
        char opt;
        try {
            while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
                switch (opt) {
                    case 'f':
                        fileName = opts.getValStr();
                        break;

                    case 'o':
                        outputDir = opts.getValStr();
                        break;

                    case 'm':
                        numMappers = Integer.valueOf(opts.getValStr());
                        break;

                    default:
                        usage();
                        break;
                }
            }
        } catch (ParseException pe) {
            System.err.println("Couldn't parse the command line arguments, " +
                    pe.getMessage());
            usage();
        }

        try {
            createTablesFromConf(fileName, numMappers, outputDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void usage() {
        System.out.println("Error");
        throw new RuntimeException();
    }

    public HiveTableCreator() throws MetaException {
        HiveConf hiveConf = new HiveConf(HiveTableCreator.class);
        hiveClient = new HiveMetaStoreClient(hiveConf);
    }

    public static void createTablesFromConf(final String fileName, final int numMappers, final String outputDir) throws IOException, SAXException, ParserConfigurationException, MetaException {
        TableSchemaXMLParser configParser = new TableSchemaXMLParser(fileName);
        HiveTableSchemas schemas = configParser.getHiveTableSchemas();
        HiveTableCreator tableCreator = new HiveTableCreator();

        for (HiveTableSchema hiveTableSchema : schemas) {
            //tableCreator.createTable(hiveTableSchema);
            tableCreator.generateDataForTable(hiveTableSchema, numMappers, outputDir);
        }
    }

    private void generateDataForTable(HiveTableSchema hiveTableSchema, final int numMappers, String outputDir) throws IOException {
        List<ColSpec> colSpecs = new ArrayList<ColSpec>(hiveTableSchema.getColSpecs());
        colSpecs.addAll(hiveTableSchema.getParitionColSpecs());
        DataGeneratorConf dgConf = new DataGeneratorConf.Builder()
                                        .colSpecs((ColSpec[]) colSpecs.toArray(new ColSpec[]{}))
                                        .numMappers(numMappers)
                                        .numRows(100) // TODO
                                        .outputFile(outputDir + "_" + hiveTableSchema.getName())
                                        .build();
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.runJob(dgConf);
    }

    public void createTable(HiveTableSchema hiveTableSchema) {
        Table table = new Table();
        table.setDbName(hiveTableSchema.getDatabaseName());
        table.setTableName(hiveTableSchema.getName());
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(hiveTableSchema.getFieldSchemas());
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
        table.setPartitionKeys(hiveTableSchema.getPartitionFieldSchemas());

        try {
            System.out.println("Creating table: " + table.getTableName());
            hiveClient.createTable(table);
        } catch (Exception e) {
            System.out.println("Error is because:" + e);
            e.printStackTrace();
        }


    }
}
