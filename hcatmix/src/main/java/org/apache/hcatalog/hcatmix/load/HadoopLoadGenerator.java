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
import org.apache.hcatalog.hcatmix.HCatMixUtils;
import org.apache.hcatalog.hcatmix.load.hadoop.HadoopUtils;
import org.apache.hcatalog.hcatmix.load.hadoop.IntervalResult;
import org.apache.hcatalog.hcatmix.load.hadoop.ReduceResult;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.Properties;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class launches a hadoop job to load test a system. Each map would keep on incrementing
 * the number of threads over time and repeatedly execute the task {@link org.apache.hcatalog.hcatmix.load.tasks.Task}
 * It measures time take to do the task over time and returns the statistics over time.
 */
public class HadoopLoadGenerator extends Configured implements Tool {
    public final String JOB_NAME = "hcat-load-generator";

    public static final String METASTORE_TOKEN_KEY = "metaStoreToken";
    public static final String METASTORE_TOKEN_SIGNATURE = "metaStoreTokenSig";

    private FileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(HadoopLoadGenerator.class);

    /**
     * Configuration that can be configured in properties files and their default values
     */
    public enum Conf {
        NUM_MAPPERS("num.mappers", 30),
        THREAD_INCREMENT_COUNT("thread.increment.count", 5),
        THREAD_INCREMENT_INTERVAL_MINUTES("thread.increment.interval.minutes", 1),
        THREAD_COMPLETION_BUFFER_MINUTES("thread.completion.buffer.minutes", 1),
        MAP_RUN_TIME_MINUTES("map.runtime.minutes", 3),
        STAT_COLLECTION_INTERVAL_MINUTE("stat.collection.interval.minutes", 2),
        INPUT_DIR("input.dir", "/tmp/hcatmix/loadtest/input"),
        OUTPUT_DIR("output.dir", "/tmp/hcatmix/loadtest/output"),
        TASK_CLASS_NAMES("task.class.names", null);

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
    public int run(String[] args) throws Exception {
        CmdLineParser opts = new CmdLineParser(args);
        String confFileName = null;

        opts.registerOpt('c', "confFile", CmdLineParser.ValueExpected.REQUIRED);

        char opt;
        try {
            while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
                switch (opt) {
                    case 'c':
                        confFileName = opts.getValStr();
                        break;
                    default:
                        throw new IllegalArgumentException("Unrecognized option");
                }
            }
        } catch (ParseException pe) {
            System.err.println("Couldn't parse the command line arguments, " +
                    pe.getMessage());
            usage();
        }
        runLoadTest(confFileName, getConf());
        return 1;
    }

    private void usage() {
        System.out.println("TODO");
        System.exit(1);
    }

    /**
     * Prepare input directory/jobConf and launch the hadoop job, for load testing
     *
     * @param confFileName The properties file for the task, should be available in the classpath
     * @param conf
     * @return
     * @throws IOException
     * @throws MetaException
     * @throws TException
     */
    public SortedMap<Long, ReduceResult> runLoadTest(String confFileName, Configuration conf) throws Exception, MetaException, TException {
        JobConf jobConf;
        if(conf != null) {
            jobConf = new JobConf(conf);
        } else {
            jobConf = new JobConf(new Configuration());
        }
        InputStream confFileIS;
        try {
            confFileIS = HCatMixUtils.getInputStream(confFileName);
        } catch (Exception e) {
            LOG.error("Couldn't load configuration file " + confFileName);
            throw e;
        }
        Properties props = new Properties();
        try {
            props.load(confFileIS);
        } catch (IOException e) {
            LOG.error("Couldn't load properties file: " + confFileName, e);
            throw e;
        }

        LOG.info("Loading configuration file: " + confFileName);
        addToJobConf(jobConf, props, Conf.MAP_RUN_TIME_MINUTES);
        addToJobConf(jobConf, props, Conf.STAT_COLLECTION_INTERVAL_MINUTE);
        addToJobConf(jobConf, props, Conf.THREAD_INCREMENT_COUNT);
        addToJobConf(jobConf, props, Conf.THREAD_INCREMENT_INTERVAL_MINUTES);
        addToJobConf(jobConf, props, Conf.THREAD_COMPLETION_BUFFER_MINUTES);

        int numMappers = Integer.parseInt(props.getProperty(Conf.NUM_MAPPERS.propName, "" + Conf.NUM_MAPPERS.defaultValue));
        Path inputDir = new Path(props.getProperty(Conf.INPUT_DIR.propName, Conf.INPUT_DIR.defaultValueStr));
        Path outputDir = new Path(props.getProperty(Conf.OUTPUT_DIR.propName, Conf.OUTPUT_DIR.defaultValueStr));

        jobConf.setJobName(JOB_NAME);
        jobConf.setNumMapTasks(numMappers);
        jobConf.setMapperClass(HCatMapper.class);
        jobConf.setJarByClass(HCatMapper.class);
        jobConf.setReducerClass(HCatReducer.class);
        jobConf.setMapOutputKeyClass(LongWritable.class);
        jobConf.setMapOutputValueClass(IntervalResult.class);
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(ReduceResult.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);
        jobConf.set(Conf.TASK_CLASS_NAMES.getJobConfKey(), props.getProperty(Conf.TASK_CLASS_NAMES.propName,
                Conf.TASK_CLASS_NAMES.defaultValueStr));

        fs = FileSystem.get(jobConf);
        Path jarRoot = new Path("/tmp/hcatmix_jar_" + new Random().nextInt());
        HadoopUtils.uploadClasspathAndAddToJobConf(jobConf, jarRoot);
        fs.deleteOnExit(jarRoot);

        FileInputFormat.setInputPaths(jobConf, createInputFiles(inputDir, numMappers));
        if(fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        FileOutputFormat.setOutputPath(jobConf, outputDir);

        // Set up delegation token required for hiveMetaStoreClient in map task
        HiveConf hiveConf = new HiveConf( HadoopLoadGenerator.class);
        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);
        String tokenStr = hiveClient.getDelegationToken(UserGroupInformation.getCurrentUser().getUserName(), "mapred");
        Token<? extends AbstractDelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
        token.decodeFromUrlString(tokenStr);
        token.setService(new Text(METASTORE_TOKEN_SIGNATURE));
        jobConf.getCredentials().addToken(new Text(METASTORE_TOKEN_KEY), token);

        // Submit the job, once the job is complete see output
        LOG.info("Submitted hadoop job");
        RunningJob j = JobClient.runJob(jobConf);
        LOG.info("JobID is: " + j.getJobName());
        if (!j.isSuccessful()) {
            throw new IOException("Job failed");
        }
        return readResult(outputDir, jobConf);
    }

    /**
     * Read result from HDFS reduce output directory and return the results
     * @param outputDir where to read the data from. Expects the file to be {SequenceFile}
     * @param jobConf
     * @return
     * @throws IOException
     */
    private SortedMap<Long, ReduceResult> readResult(Path outputDir, JobConf jobConf) throws IOException {
        SortedMap<Long, ReduceResult> timeseriesResults = new TreeMap<Long, ReduceResult>();
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
            ReduceResult result = new ReduceResult();
            while (reader.next(timeStamp, result)) {
                LOG.info("Timestamp: " + timeStamp);
                LOG.info("ThreadCount: " + result.getThreadCount());
                LOG.info("Stats:\n" + result.getStatistics());
                LOG.info("Errors: " + result.getNumErrors());
                timeseriesResults.put(timeStamp.get(), result);
                timeStamp = new LongWritable(); // initialize, so as to use new objects for next round reading
                result = new ReduceResult();
            }
            reader.close();
        }
        return timeseriesResults;
    }

    private static void addToJobConf(JobConf jobConf, Properties props, Conf conf) {
        jobConf.set(conf.getJobConfKey(), props.getProperty(conf.propName, "" + conf.defaultValue));
    }


    /**
     * Create input directory with dummy input file to match the number of mappers
     * @param inputDir
     * @param numMappers
     * @return
     * @throws IOException
     */
    private Path[] createInputFiles(final Path inputDir, final int numMappers) throws IOException {
        Path[] paths = new Path[numMappers];
        if (!fs.exists(inputDir)) {
            LOG.info("Input Directory doesn't exist will create input dir: " + inputDir);
            if(!fs.mkdirs(inputDir)) {
                HCatMixUtils.logAndThrow(new RuntimeException("Couldn't create input directory: " + inputDir));
            }
        } else {
            LOG.info("Input directory already exists, skipping creation : " + inputDir);
        }

        for (int i = 0; i < numMappers; i++) {
            Path childDir = new Path(inputDir, "input_" + i);
            if(!fs.exists(childDir)) {
                if(!fs.mkdirs(childDir)) {
                    HCatMixUtils.logAndThrow(new RuntimeException("Couldn't create input child directory: " + childDir));
                }
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
