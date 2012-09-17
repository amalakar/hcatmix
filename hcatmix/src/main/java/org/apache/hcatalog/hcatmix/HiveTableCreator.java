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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;
import org.apache.hcatalog.hcatmix.conf.TableSchemaXMLParser;
import org.apache.pig.test.utils.datagen.ColSpec;
import org.apache.pig.test.utils.datagen.DataGenerator;
import org.apache.pig.test.utils.datagen.DataGeneratorConf;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HiveTableCreator extends Configured implements Tool {
    HiveMetaStoreClient hiveClient;
    public final static char SEPARATOR = ',';

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new HiveTableCreator(), args);
    }

    public static void usage() {
        System.out.println("Error");
        throw new RuntimeException();
    }

    public HiveTableCreator() throws MetaException {
        HiveConf hiveConf = new HiveConf(HiveTableCreator.class);
        hiveClient = new HiveMetaStoreClient(hiveConf);
    }

    public void createTablesFromConf(final String fileName, final int numMappers, final String outputDir, final String pigScriptDir) throws IOException, SAXException, ParserConfigurationException, MetaException {
        TableSchemaXMLParser configParser = new TableSchemaXMLParser(fileName);
        List<HiveTableSchema> multiInstanceList = configParser.getHiveTableList();
        for (HiveTableSchema hiveTableSchema : multiInstanceList) {
            createTable(hiveTableSchema);
            generateDataForTable(hiveTableSchema, numMappers, outputDir);
            String loadScript = PigScriptGenerator.getPigLoadScript(HCatMixUtils.getDataLocation(outputDir, hiveTableSchema), hiveTableSchema);
            FileUtils.writeStringToFile(new File(HCatMixUtils.getPigLoadScriptName(pigScriptDir, hiveTableSchema.getName())),
                loadScript);
//            File pigLoadScript = new FileOutputStream());


            //PigScriptGenerator.getPigLoadScript(HCatMixUtils.getDataLocation(outputDir, hiveTableSchema), hiveTableSchema);
        }
    }

    private void generateDataForTable(HiveTableSchema hiveTableSchema, final int numMappers, String outputDir) throws IOException {
        List<ColSpec> colSpecs = new ArrayList<ColSpec>(hiveTableSchema.getColumnColSpecs());
        colSpecs.addAll(hiveTableSchema.getPartitionColSpecs());
        DataGeneratorConf dgConf = new DataGeneratorConf.Builder()
                                        .colSpecs(colSpecs.toArray(new ColSpec[colSpecs.size()]))
                                        .separator(SEPARATOR)
                                        .numMappers(numMappers)
                                        .numRows(hiveTableSchema.getRowCount()) // TODO
                                        .outputFile(HCatMixUtils.getDataLocation(outputDir, hiveTableSchema))
                                        .build();
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.runJob(dgConf, getConf());
    }

    public void createTable(HiveTableSchema hiveTableSchema) {
        Table table = new Table();
        table.setDbName(hiveTableSchema.getDatabaseName());
        table.setTableName(hiveTableSchema.getName());
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(hiveTableSchema.getColumnFieldSchemas());
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

    @Override
    public int run(String[] args) throws Exception {
        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('m', "mappers", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('o', "output", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('p', "pig-script-output-dir", CmdLineParser.ValueExpected.REQUIRED);

        String fileName = null;
        int numMappers = 0;
        String outputDir = null;
        String pigScriptDir = null;
        char opt;
        try {
            while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
                switch (opt) {
                    case 'f':
                        fileName = opts.getValStr();
                        break;

                    case 'o':
                        outputDir = opts.getValStr();
                        if(!outputDir.endsWith("/")) {
                            outputDir = outputDir + "/";
                        }
                        break;

                    case 'm':
                        numMappers = Integer.valueOf(opts.getValStr());
                        break;

                    case 'p':
                        pigScriptDir = opts.getValStr();
                        if(!pigScriptDir.endsWith("/")) {
                            pigScriptDir += "/";
                        }
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
            createTablesFromConf(fileName, numMappers, outputDir, pigScriptDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
