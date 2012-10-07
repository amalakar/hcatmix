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
import org.perf4j.GroupedTimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

public class HadoopLoadGenerator extends Configured implements Tool {
    public final String JOB_NAME = "hcat-load-generator";
    public final int NUM_MAPPERS = 30;
    public final Path OUTPUT_DIR = new Path("/tmp/hcatmix/load/output");
    public final String INPUT_DIR = "/tmp/hcatmix/load/input";

    public static final String METASTORE_TOKEN_KEY = "metaStoreToken";
    public static final String METASTORE_TOKEN_SIGNATURE = "metaStoreTokenSig";

    private FileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(HadoopLoadGenerator.class);

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


        jobConf.setJobName(JOB_NAME);
        jobConf.setNumMapTasks(NUM_MAPPERS);
        jobConf.setMapperClass(HCatMapper.class);
        jobConf.setJarByClass(HCatMapper.class);
        jobConf.setReducerClass(HCatReducer.class);
        jobConf.setMapOutputKeyClass(LongWritable.class);
        jobConf.setMapOutputValueClass(StopWatchWritable.ArrayStopWatchWritable.class);
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(GroupedTimingStatistics.class);
        fs = FileSystem.get(jobConf);

        FileInputFormat.setInputPaths(jobConf, createInputFiles(INPUT_DIR, NUM_MAPPERS));
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
