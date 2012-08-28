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
        job.set("fieldconfig", config);
        job.set("separator", String.valueOf((int) dgConf.getSeparator()));


        job.setJobName("data-gen");
        job.setNumMapTasks(dgConf.getNumMappers());
        job.setNumReduceTasks(0);
        job.setMapperClass(DataGenMapper.class);
        job.setJarByClass(DataGenMapper.class);

        // if inFile is specified, use it as input
        if (dgConf.getInFile() != null) {
            FileInputFormat.setInputPaths(job,dgConf.getInFile());
            job.set("hasinput", "true");
        } else {
            job.set("hasinput", "false");
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
            Object[] tmp = createTempFile(input, false);
            PrintWriter pw = new PrintWriter((OutputStream) tmp[1]);

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

    // generate map files for all the fields that need to pre-generate map files
    // return a config file which contains config info for each field, including
    // the path to their map file
    private Path genMapFiles() throws IOException {
        Object[] tmp = createTempFile(tmpHome, false);

        System.out.println("Generating column config file in " + tmp[0].toString());
        PrintWriter pw = new PrintWriter((OutputStream) tmp[1]);
        for (int i = 0; i < dgConf.getColSpecs().length; i++) {
            DataType dataType = dgConf.getColSpecs()[i].getDataType();
            pw.print(dgConf.getColSpecs()[i].getStringRepresentation());

            if (dataType == DataType.FLOAT || dataType == DataType.DOUBLE ||
                    dataType == DataType.STRING) {
                Path p = genMapFile(dgConf.getColSpecs()[i]);
                pw.print(':');
                pw.print(p.toUri().getRawPath());
            }

            pw.println();
        }

        pw.close();

        return (Path) tmp[0];
    }

    // genereate a map file between random number to field value
    // return the path of the map file
    private Path genMapFile(ColSpec col) throws IOException {
        int cardinality = col.getCardinality();
        Object[] tmp = createTempFile(tmpHome, false);

        System.out.println("Generating mapping file for column " + col.getStringRepresentation() + " into " + tmp[0].toString());
        PrintWriter pw = new PrintWriter((OutputStream) tmp[1]);
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

        return (Path) tmp[0];

    }

    private Path createTempDir(Path parentDir) throws IOException {
        Object[] obj = createTempFile(parentDir, true);
        return (Path) obj[0];
    }

    private Object[] createTempFile(Path parentDir, boolean isDir) throws IOException {
        Path tmp_home = parentDir;

        if (tmp_home == null) {
            tmp_home = new Path(fs.getHomeDirectory(), "tmp");
        }

        if (!fs.exists(tmp_home)) {
            fs.mkdirs(tmp_home);
        }

        int id = r.nextInt();
        Path f = new Path(tmp_home, "tmp" + id);
        while (fs.exists(f)) {
            id = r.nextInt();
            f = new Path(tmp_home, "tmp" + id);
        }

        // return a 2-element array. first element is PATH,
        // second element is OutputStream
        Object[] r = new Object[2];
        r[0] = f;
        if (!isDir) {
            r[1] = fs.create(f);
        } else {
            fs.mkdirs(f);
        }

        return r;
    }
}
