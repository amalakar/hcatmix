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

package org.apache.hcatalog.hcatmix.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Properties;

public class HadoopLoadGenerator extends Configured implements Tool {
    public static final String CONF_FILE = "hcat_load_test.properties";
    public final String JOB_NAME = "hcat-load-generator";
    public final Path OUTPUT_DIR = new Path("/tmp/hcatmix/load/output");
    public final String INPUT_DIR = "/tmp/hcatmix/load/input";

    public static final String METASTORE_TOKEN_KEY = "metaStoreToken";
    public static final String METASTORE_TOKEN_SIGNATURE = "metaStoreTokenSig";

    private FileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(HadoopLoadGenerator.class);

    public enum Conf {
        NUM_MAPPERS("num.mappers", 30),
        THREAD_INCREMENT_COUNT("thread.increment.count", 5),
        THREAD_INCREMENT_INTERVAL_MINUTES("thread.increment.interval.minutes", 1),
        MAP_RUN_TIME("thread.increment.count", 3),
        THREAD_COMPLETION_BUFFER("thread.completion.buffer.minutes", 1),
        STAT_COLLECTION_INTERVAL_MINUTE("stat.collection.interval.minutes", 2);

        public final String propName;
        public final int defaultValue;

        Conf(final String propName, final int defaultVale) {
            this.propName = propName;
            this.defaultValue = defaultVale;

        }

        public String getJobConfKey() {
            return "hcatmix." + propName;
        }
    }

    public HadoopLoadGenerator() {
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new HadoopLoadGenerator(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        return run();
    }

    public int run() throws IOException, MetaException, TException {

        // Create a JobConf using the processed conf
        JobConf jobConf;
        if(getConf() != null) {
            jobConf = new JobConf(getConf());
        } else {
            jobConf = new JobConf();
        }
        InputStream confFile = HCatMapper.class.getResourceAsStream(CONF_FILE);
        int numMappers = Conf.NUM_MAPPERS.defaultValue;
        if(confFile != null) {
            Properties props = new Properties();
            try {
                props.load(confFile);
            } catch (IOException e) {
                LOG.error("[Ignored] Couldn't load properties file: " + CONF_FILE, e);
            }

            numMappers = Integer.parseInt(props.getProperty(Conf.NUM_MAPPERS.propName, "" + Conf.NUM_MAPPERS.defaultValue));
            addToJobConf(jobConf, props, Conf.MAP_RUN_TIME);
            addToJobConf(jobConf, props, Conf.STAT_COLLECTION_INTERVAL_MINUTE);
            addToJobConf(jobConf, props, Conf.THREAD_INCREMENT_COUNT);
            addToJobConf(jobConf, props, Conf.THREAD_INCREMENT_INTERVAL_MINUTES);
            addToJobConf(jobConf, props, Conf.THREAD_COMPLETION_BUFFER);
        } else {
            LOG.error("Couldn't find config file in classpath: " + CONF_FILE + " ignored and proceeding");
        }

        jobConf.setJobName(JOB_NAME);
        jobConf.setNumMapTasks(numMappers);
        jobConf.setMapperClass(HCatMapper.class);
        jobConf.setJarByClass(HCatMapper.class);
        jobConf.setReducerClass(HCatReducer.class);
        jobConf.setMapOutputKeyClass(LongWritable.class);
        jobConf.setMapOutputValueClass(StopWatchWritable.MapResult.class);
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(Text.class);
        fs = FileSystem.get(jobConf);

        FileInputFormat.setInputPaths(jobConf, createInputFiles(INPUT_DIR, numMappers));
        if(fs.exists(OUTPUT_DIR)) {
            fs.delete(OUTPUT_DIR, true);
        }
        FileOutputFormat.setOutputPath(jobConf, OUTPUT_DIR);

        // Set up delegation token required for hiveMetaStoreClient in map task
        HiveConf hiveConf = new HiveConf(Task.class);
        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);
        String tokenStr = hiveClient.getDelegationToken(UserGroupInformation.getCurrentUser().getUserName(), "mapred");
        Token<? extends AbstractDelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
        token.decodeFromUrlString(tokenStr);
        token.setService(new Text(METASTORE_TOKEN_SIGNATURE));
        jobConf.getCredentials().addToken(new Text(METASTORE_TOKEN_KEY), token);

        // Submit the job, then poll for progress until the job is complete
        LOG.info("Submitted hadoop job");
        RunningJob j = JobClient.runJob(jobConf);
        LOG.info("JobID is: " + j.getJobName());
        if (!j.isSuccessful()) {
            throw new IOException("Job failed");
        }
        return 0;
    }

    private static void addToJobConf(JobConf jobConf, Properties props, Conf conf) {
        jobConf.set(conf.getJobConfKey(), props.getProperty(conf.propName, "" + conf.defaultValue));
    }

    private Path[] createInputFiles(final String inputDirName, final int numMappers) throws IOException {
        Path inputDir = new Path(inputDirName);
        Path[] paths = new Path[numMappers];
        if (!fs.exists(inputDir)) {
            LOG.info("Directory doesn't exist will create input dir: " + inputDirName);
            fs.mkdirs(inputDir);
        } else {
            LOG.info("Input directory already exists, skipping creation : " + inputDirName);
        }

        for (int i = 0; i < numMappers; i++) {
            Path childDir = new Path(inputDir, "input_" + i);
            if(!fs.exists(childDir)) {
                fs.mkdirs(childDir);
            }
            Path childFile = new Path(childDir, "input");
            if(!fs.exists(childFile)) {
                OutputStream out = fs.create(childFile);
                PrintWriter pw = new PrintWriter(out);
                pw.println("Dummy Input");
                pw.close();
            }
            paths[i] = childDir;
        }

        return paths;
    }
}
