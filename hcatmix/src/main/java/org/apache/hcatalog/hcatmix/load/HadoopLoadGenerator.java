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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

public class HadoopLoadGenerator extends Configured implements Tool {
    public final String JOB_NAME = "hcat-load-generator";
    public final int NUM_MAPPERS = 30;
    public final String OUTPUT_DIR = "/tmp/hcatmix/load/output";
    public final String INPUT_DIR = "/tmp/hcatmix/load/input";

    private FileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(HadoopLoadGenerator.class);

    public HadoopLoadGenerator() {
    }

    @Override
    public int run(String[] strings) throws Exception {
        return run();
    }

    public int run() throws IOException {

        // Create a JobConf using the processed conf
        JobConf job = new JobConf(getConf());

        job.setJobName(JOB_NAME);
        job.setNumMapTasks(NUM_MAPPERS);
        job.setMapperClass(HCatMapper.class);
        job.setReducerClass(HCatReducer.class);

        fs = FileSystem.get(job);

        FileInputFormat.setInputPaths(job, createInputFiles(INPUT_DIR, NUM_MAPPERS));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

        // Submit the job, then poll for progress until the job is complete
        LOG.info("Submitted hadoop job");
        RunningJob j = JobClient.runJob(job);
        LOG.info("JobID is: " + j.getJobName());
        if (!j.isSuccessful()) {
            throw new IOException("Job failed");
        }
        return 0;
    }

    private Path createInputFiles(final String inputDirName, final int numMappers) throws IOException {
        Path inputDir = new Path(inputDirName);

        if (!fs.exists(inputDir)) {
            fs.mkdirs(inputDir);
        }

        for (int i = 0; i < numMappers; i++) {
            Path childDir = new Path(inputDir, "input_" + i);
            fs.mkdirs(childDir);
            Path childFile = new Path(childDir, "input");
            OutputStream out = fs.create(childFile);
            PrintWriter pw = new PrintWriter(out);
            pw.println("Dummy Input");
            pw.close();
        }
        return inputDir;
    }
}
