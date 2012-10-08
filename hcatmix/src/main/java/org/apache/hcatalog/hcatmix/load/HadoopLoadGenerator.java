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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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

    public static final String METASTORE_TOKEN_KEY = "metaStoreToken";
    public static final String METASTORE_TOKEN_SIGNATURE = "metaStoreTokenSig";

    private FileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(HadoopLoadGenerator.class);

    public enum Conf {
        NUM_MAPPERS("num.mappers", 30),
        THREAD_INCREMENT_COUNT("thread.increment.count", 5),
        THREAD_INCREMENT_INTERVAL_MINUTES("thread.increment.interval.minutes", 1),
        THREAD_COMPLETION_BUFFER_MINUTES("thread.completion.buffer.minutes", 1),
        MAP_RUN_TIME_MINUTES("map.runtime.minutes", 3),
        STAT_COLLECTION_INTERVAL_MINUTE("stat.collection.interval.minutes", 2),
        INPUT_DIR("input.dir", "/tmp/hcatmix/loadtest/input"),
        OUTPUT_DIR("output.dir", "/tmp/hcatmix/loadtest/input");

        public final String propName;
        public final int defaultValue;
        public final String defaultValueStr;

        Conf(final String propName, final int defaultVale) {
            this.propName = propName;
            this.defaultValue = defaultVale;
            this.defaultValueStr = null;
        }

        Conf(final String propName, final String defaultValue) {
            this.propName = propName;
            this.defaultValue = -1;
            this.defaultValueStr = defaultValue;
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
        InputStream confFile = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_FILE);

        Properties props = new Properties();
        if(confFile != null) {
            try {
                props.load(confFile);
            } catch (IOException e) {
                LOG.error("[Ignored] Couldn't load properties file: " + CONF_FILE, e);
            }

            LOG.info("Loading configuration file: " + CONF_FILE);
            addToJobConf(jobConf, props, Conf.MAP_RUN_TIME_MINUTES);
            addToJobConf(jobConf, props, Conf.STAT_COLLECTION_INTERVAL_MINUTE);
            addToJobConf(jobConf, props, Conf.THREAD_INCREMENT_COUNT);
            addToJobConf(jobConf, props, Conf.THREAD_INCREMENT_INTERVAL_MINUTES);
            addToJobConf(jobConf, props, Conf.THREAD_COMPLETION_BUFFER_MINUTES);
        } else {
            LOG.error("Couldn't find config file in classpath: " + CONF_FILE + " ignored and proceeding");
        }

        int numMappers = Integer.parseInt(props.getProperty(Conf.NUM_MAPPERS.propName, "" + Conf.NUM_MAPPERS.defaultValue));
        Path inputDir = new Path(props.getProperty(Conf.INPUT_DIR.propName, Conf.INPUT_DIR.defaultValueStr));
        Path outputDir = new Path(props.getProperty(Conf.OUTPUT_DIR.propName, Conf.OUTPUT_DIR.defaultValueStr));

        jobConf.setJobName(JOB_NAME);
        jobConf.setNumMapTasks(numMappers);
        jobConf.setMapperClass(HCatMapper.class);
        jobConf.setJarByClass(HCatMapper.class);
        jobConf.setReducerClass(HCatReducer.class);
        jobConf.setMapOutputKeyClass(LongWritable.class);
        jobConf.setMapOutputValueClass(StopWatchWritable.MapResult.class);
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(StopWatchWritable.ReduceResult.class);
        fs = FileSystem.get(jobConf);

        FileInputFormat.setInputPaths(jobConf, createInputFiles(inputDir, numMappers));
        if(fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(jobConf, outputDir);

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

        readResult(outputDir, jobConf);
        return 0;
    }

    public void readResult(Path outputDir, JobConf jobConf) throws IOException {
        FileStatus[] files = fs.listStatus(outputDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith("part");
            }
        });
        for (FileStatus status : files) {
            Path path = status.getPath();
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, jobConf);
            LongWritable timeStamp = new LongWritable();
            StopWatchWritable.ReduceResult result = new StopWatchWritable.ReduceResult();
            while (reader.next(timeStamp, result)) {
                LOG.info("Timestamp: " + timeStamp);
                LOG.info("ThreadCount: " +result.getThreadCount());
                LOG.info("Stats:\n" + result.getStatistics());
            }
            reader.close();
        }
    }

    private static void addToJobConf(JobConf jobConf, Properties props, Conf conf) {
        jobConf.set(conf.getJobConfKey(), props.getProperty(conf.propName, "" + conf.defaultValue));
    }

    private Path[] createInputFiles(final Path inputDir, final int numMappers) throws IOException {
        Path[] paths = new Path[numMappers];
        if (!fs.exists(inputDir)) {
            LOG.info("Directory doesn't exist will create input dir: " + inputDir);
            fs.mkdirs(inputDir);
        } else {
            LOG.info("Input directory already exists, skipping creation : " + inputDir);
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
