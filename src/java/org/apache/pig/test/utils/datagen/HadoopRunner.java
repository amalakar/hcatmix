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

package org.apache.pig.test.utils.datagen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.pig.test.utils.DataType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Random;

public class HadoopRunner {
    Random r;
    FileSystem fs;
    Path tmpHome;
    private DataGeneratorConf dgConf;
    private Configuration conf;

    public final static String COLUMN_CONF_FILE_PATH = "column_conf_file_path";
    public final static String COLUMN_OUTPUT_SEPARATOR = "column_output_separator";
    public final static String HAS_USER_INPUT = "hasinput";

    public HadoopRunner(DataGeneratorConf dgConf, Configuration conf) {
        this.dgConf = dgConf;
        this.conf = conf;
        r = new Random();
    }

    public void generate() throws IOException {
        // Configuration processed by ToolRunner

        // Create a JobConf using the processed conf
        JobConf job = new JobConf(conf);
        fs = FileSystem.get(job);

        tmpHome = createTempDir(null);

        String config = genMapFiles().toUri().getRawPath();
        // set config properties into job conf
        job.set(COLUMN_CONF_FILE_PATH, config);
        job.set(COLUMN_OUTPUT_SEPARATOR, String.valueOf((int) dgConf.getSeparator()));

        job.setJobName("data-gen");
        job.setNumMapTasks(dgConf.getNumMappers());
        job.setNumReduceTasks(0);
        job.setMapperClass(DataGenMapper.class);
        job.setJarByClass(DataGenMapper.class);

        // if inFile is specified, use it as input
        if (dgConf.getInFile() != null) {
            FileInputFormat.setInputPaths(job,dgConf.getInFile());
            job.set(HAS_USER_INPUT, "true");
        } else {
            job.set(HAS_USER_INPUT, "false");
            Path input = genInputFiles();
            FileInputFormat.setInputPaths(job, input);
        }
        FileOutputFormat.setOutputPath(job, new Path(dgConf.getOutputFile()));

        // Submit the job, then poll for progress until the job is complete
        System.out.println("Submit hadoop job...");
        RunningJob j = JobClient.runJob(job);
        if (!j.isSuccessful()) {
            throw new IOException("Job failed");
        }

        if (fs.exists(tmpHome)) {
            fs.delete(tmpHome, true);
        }
    }

    private Path genInputFiles() throws IOException {
        long avgRows = dgConf.getNumRows() / dgConf.getNumMappers();

        // create a temp directory as mappers input
        Path input = createTempDir(tmpHome);
        System.out.println("Generating input files into " + input.toString());

        long rowsLeft = dgConf.getNumRows();

        // create one input file per mapper, which contains
        // the number of rows
        for (int i = 0; i < dgConf.getNumMappers(); i++) {
            TempFile tmpFile = createTempFile(input, false);
            PrintWriter pw = new PrintWriter(tmpFile.outputStream);

            if (i < dgConf.getNumMappers() - 1) {
                pw.println(avgRows);
            } else {
                // last mapper takes all the rows left
                pw.println(rowsLeft);
            }

            pw.close();
            rowsLeft -= avgRows;
        }

        return input;
    }

    /**
     * Generate map files for all the fields that need to pre-generate map files
     * return a config file which contains config info for each field, including
     * the path to their map file
     * @return
     * @throws IOException
     */
    private Path genMapFiles() throws IOException {
        TempFile tmpFile = createTempFile(tmpHome, false);

        System.out.println("Generating column config file in " + tmpFile.path.toString());
        PrintWriter pw = new PrintWriter(tmpFile.outputStream);
        for (int i = 0; i < dgConf.getColSpecs().length; i++) {
            DataType dataType = dgConf.getColSpecs()[i].getDataType();
            pw.print(dgConf.getColSpecs()[i].getStringRepresentation());

            if (dataType == DataType.FLOAT || dataType == DataType.DOUBLE ||
                    dataType == DataType.STRING) {
                Path p = genSampleSpaceAndMapping(dgConf.getColSpecs()[i]);
                pw.print(ColSpec.SEPARATOR);
                pw.print(p.toUri().getRawPath());
            }
            pw.println();
        }

        pw.close();

        return tmpFile.path;
    }

    /**
     * Generates the sample space for DOUBLE, FLOAT and STRING
     * and the mapping for the same from integers ranging from 0 to cardinality.
     * Writes this onto a temp file and returns the Path to it
     */
    private Path genSampleSpaceAndMapping(ColSpec col) throws IOException {
        int cardinality = col.getCardinality();
        TempFile tmpFile = createTempFile(tmpHome, false);

        System.out.println("Generating mapping file for column " + col + " into " + tmpFile.path.toString());
        PrintWriter pw = new PrintWriter(tmpFile.outputStream);
        HashSet<Object> hash = new HashSet<Object>(cardinality);
        for (int i = 0; i < cardinality; i++) {
            pw.print(i);
            pw.print("\t");
            Object next = null;
            do {
                if (col.getDataType() == DataType.DOUBLE) {
                    next = col.getGen().randomDouble();
                } else if (col.getDataType() == DataType.FLOAT) {
                    next = col.getGen().randomFloat();
                } else if (col.getDataType() == DataType.STRING) {
                    next = col.getGen().randomString();
                }
            } while (hash.contains(next));

            hash.add(next);

            pw.println(next);

            if ((i > 0 && i % 300000 == 0) || i == cardinality - 1) {
                System.out.println("processed " + i * 100 / cardinality + "%.");
                pw.flush();
            }
        }

        pw.close();

        return tmpFile.path;

    }

    private Path createTempDir(Path parentDir) throws IOException {
        return createTempFile(parentDir, true).path;
    }

    private static class TempFile {
        public final Path path;
        public final OutputStream outputStream;

        private TempFile(Path path, OutputStream outputStream) throws IOException {
            this.path = path;
            this.outputStream = outputStream;
        }
    }

    private TempFile createTempFile(Path parentDir, boolean isDir) throws IOException {
        Path tmpHome = parentDir;
        if (tmpHome == null) {
            tmpHome = new Path(fs.getHomeDirectory(), "tmp");
        }

        if (!fs.exists(tmpHome)) {
            fs.mkdirs(tmpHome);
        }

        int id = r.nextInt();
        Path path = new Path(tmpHome, "tmp" + id);
        while (fs.exists(path)) {
            id = r.nextInt();
            path = new Path(tmpHome, "tmp" + id);
        }
        TempFile tmpFile;
        if (isDir) {
            fs.mkdirs(path);
            tmpFile = new TempFile(path, null);
        } else {
            tmpFile = new TempFile(path, fs.create(path));
        }
        return tmpFile;
    }
}
