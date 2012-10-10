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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;
import org.apache.hcatalog.hcatmix.conf.TableSchemaXMLParser;
import org.apache.pig.test.utils.datagen.ColSpec;
import org.apache.pig.test.utils.datagen.DataGenerator;
import org.apache.pig.test.utils.datagen.DataGeneratorConf;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Main class to
 *      1. create hcat tables
 *      2. Generate data for it
 *      3. Generate pig scripts to load the data in hcat tables
 * This class is named HCatMixSetup as it does the initial setup for doing performance test
 */

public class HCatMixSetup extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(HCatMixSetup.class);

    HiveMetaStoreClient hiveClient;
    public final static char SEPARATOR = ',';

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new HCatMixSetup(), args);
    }

    public static void usage() {
        System.err.println("Error");
        throw new RuntimeException();
    }

    public HCatMixSetup() throws MetaException {
        HiveConf hiveConf = new HiveConf(HCatMixSetup.class);
        hiveClient = new HiveMetaStoreClient(hiveConf);
    }

    public void setupFromConf(HCatMixSetupConf conf)
            throws IOException, SAXException, ParserConfigurationException, MetaException, NoSuchObjectException, TException, InvalidObjectException {

        TableSchemaXMLParser configParser = new TableSchemaXMLParser(conf.getConfFileName());
        List<HiveTableSchema> multiInstanceList = configParser.getHiveTableList();
        for (HiveTableSchema hiveTableSchema : multiInstanceList) {
            if(conf.isCreateTable()) {
                try {
                    createTable(hiveTableSchema);
                } catch (AlreadyExistsException e) {
                    LOG.info("Couldn't create table, " + hiveTableSchema.getName() + ". Already exists ignored and proceeding", e);
                }
            }

            if(conf.isGenerateData()) {
                generateDataForTable(hiveTableSchema, conf.getNumMappers(), conf.getOutputDir());
            }

            if(conf.isGeneratePigScripts()) {
                generatePigScripts(conf.getOutputDir(), conf.getPigDataOutputDir(), hiveTableSchema, conf.getPigScriptDir());
            }
        }
    }

    public void generatePigScripts(final String outputDir, final String pigOutputDir,
                                    final HiveTableSchema hiveTableSchema, final String pigScriptDir ) throws IOException {
        PigScriptGenerator pigScriptGenerator = new PigScriptGenerator(HCatMixUtils.getDataLocation(outputDir, hiveTableSchema.getName()),
                pigOutputDir, hiveTableSchema);

        LOG.info(MessageFormat.format("About to generate pig scripts in {0}, for table: {1} for input data in location: {2}",
                pigScriptDir, hiveTableSchema.getName(), outputDir));

        // 1. Script for loading using pig store using hcatStorer()
        final String pigLoadHCatStoreScript = HCatMixUtils.getHCatStoreScriptName(pigScriptDir, hiveTableSchema.getName());
        FileUtils.writeStringToFile(new File(pigLoadHCatStoreScript), pigScriptGenerator.getPigLoaderHCatStorerScript());
        LOG.info(MessageFormat.format("1. Successfully created the pig loader/hcat storer script: {0}", pigLoadHCatStoreScript));

        // 2. Script for loading/storing using pigstorage()
        final String pigLoadPigStorerScript = HCatMixUtils.getPigLoadStoreScriptName(pigScriptDir, hiveTableSchema.getName());
        FileUtils.writeStringToFile(new File(pigLoadPigStorerScript), pigScriptGenerator.getPigLoaderPigStorerScript());
        LOG.info(MessageFormat.format("2. Successfully created the pig loader/pig storer script: {0}", pigLoadPigStorerScript));

        // 3. Script for loading using HCatLoader() and store using pigStorage()
        final String hcatLoadPigStorerScript = HCatMixUtils.getHCatLoadScriptName(pigScriptDir, hiveTableSchema.getName());
        FileUtils.writeStringToFile(new File(hcatLoadPigStorerScript), pigScriptGenerator.getHCatLoaderPigStorerScript());
        LOG.info(MessageFormat.format("3. Successfully created the hcat loader/pig storer script: {0}", hcatLoadPigStorerScript));

        // 3. Script for loading using HCatLoader() and store using HCatStorage()
        final String hcatLoadHCatStorerScript = HCatMixUtils.getHCatLoadStoreScriptName(pigScriptDir, hiveTableSchema.getName());
        FileUtils.writeStringToFile(new File(hcatLoadHCatStorerScript), pigScriptGenerator.getHCatLoaderHCatStorerScript());
        LOG.info(MessageFormat.format("4. Successfully created the hcat loader/hcat storer script: {0}", hcatLoadHCatStorerScript));
    }

    public void generateDataForTable(HiveTableSchema hiveTableSchema, final int numMappers, String outputDir) throws IOException {
        String outputFile = HCatMixUtils.getDataLocation(outputDir, hiveTableSchema.getName());
        LOG.info(MessageFormat.format("About to generate data for table: {0}, with number of mappers: {1}, output location: {2}",
                hiveTableSchema.getName(), numMappers, outputFile));

        if (!HCatMixHDFSUtils.exists(outputFile)) {
            List<ColSpec> colSpecs = new ArrayList<ColSpec>(hiveTableSchema.getColumnColSpecs());
            colSpecs.addAll(hiveTableSchema.getPartitionColSpecs());
            DataGeneratorConf dgConf = new DataGeneratorConf.Builder()
                    .colSpecs(colSpecs.toArray(new ColSpec[colSpecs.size()]))
                    .separator(SEPARATOR)
                    .numMappers(numMappers)
                    .numRows(hiveTableSchema.getRowCount())
                    .outputFile(outputFile)
                    .build();
            DataGenerator dataGenerator = new DataGenerator();
            dataGenerator.runJob(dgConf, getConf());
            LOG.info(MessageFormat.format("Successfully created input data in: {0}", outputFile));
        } else {
            LOG.info(MessageFormat.format("Output location {0} already exists, skipping data generation", outputFile));
        }
    }

    public void createTable(HiveTableSchema hiveTableSchema) throws IOException, TException, NoSuchObjectException, MetaException, AlreadyExistsException, InvalidObjectException {
        LOG.info("About to create table: " + hiveTableSchema.getName());
        Table table = new Table();
        table.setDbName(hiveTableSchema.getDatabaseName());
        table.setTableName(hiveTableSchema.getName());
        try {
            table.setOwner(UserGroupInformation.getCurrentUser().getUserName());
        } catch (IOException e) {
            throw new IOException("Couldn't get user information. Cannot create table", e);
        }
        table.setOwnerIsSet(true);
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

        hiveClient.createTable(table);
        LOG.info("Successfully created table: " + table.getTableName());
    }

    public void deleteTable(String dbName, String tableName) throws TException, MetaException, NoSuchObjectException {
        LOG.info(MessageFormat.format("About to delete table: {0}.{1}", dbName, tableName));
        hiveClient.dropTable(dbName, tableName);
        LOG.info(MessageFormat.format("Successfully deleted table: {0}.{1}", dbName, tableName));
    }

    @Override
    public int run(String[] args) throws Exception {
        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('m', "mappers", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('o', "output-dir", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('p', "pig-script-output-dir", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('a', "pig-data-output-dir", CmdLineParser.ValueExpected.REQUIRED);

        opts.registerOpt('t', "create-table", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('d', "generate-data", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('s', "generate-pig-scripts", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('e', "do-everything", CmdLineParser.ValueExpected.NOT_ACCEPTED);

        HCatMixSetupConf.Builder builder = new HCatMixSetupConf.Builder();

        char opt;
        try {
            while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
                switch (opt) {
                case 'f':
                    builder.confFileName(opts.getValStr());
                    break;

                case 'o':
                    builder.outputDir(opts.getValStr());
                    break;

                case 'm':
                    builder.numMappers(Integer.valueOf(opts.getValStr()));
                    break;

                case 'p':
                    builder.pigScriptDir(opts.getValStr());
                    break;

                case 'e':
                    builder.doEverything();
                    break;

                case 't':
                    builder.createTable();
                    break;

                case 's':
                    builder.generatePigScripts();
                    break;

                case 'd':
                    builder.generateData();
                    break;

                case 'a':
                    builder.pigDataOutputDir(opts.getValStr());
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
            setupFromConf(builder.build());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
