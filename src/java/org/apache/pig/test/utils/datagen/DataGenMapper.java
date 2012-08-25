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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.pig.test.utils.DataType;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataGenMapper extends MapReduceBase implements Mapper<LongWritable, Text, String, String> {
    private JobConf jobConf;
   // private DataGenerator dg;
    private DataGeneratorConf dgConf;
    private boolean hasInput;
    Writer writer;


    @Override
    public void configure(JobConf jobconf) {
        this.jobConf = jobconf;

        int id = Integer.parseInt(jobconf.get("mapred.task.partition"));
        long time = System.currentTimeMillis() - id * 3600 * 24 * 1000;

        DataGeneratorConf.Builder dgConfBuilder = new DataGeneratorConf.Builder();

        //dg = new DataGenerator(((time - id * 3600 * 24 * 1000) | (id << 48)));
        dgConfBuilder.seed(((time - id * 3600 * 24 * 1000) | (id << 48)));

//        dg.separator = (char) Integer.parseInt(jobConf.get("separator"));
        dgConfBuilder.separator((char) Integer.parseInt(jobConf.get("separator")));
        if (jobConf.get("hasinput").equals("true")) {
            hasInput = true;
        }

        String config = jobConf.get("fieldconfig");

        try {
            FileSystem fs = FileSystem.get(jobconf);

            // load in config file for each column
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(config))));
            String line;
            List<ColSpec> cols = new ArrayList<ColSpec>();
            while ((line = reader.readLine()) != null) {
                cols.add(ColSpec.fromStringRepresentation(line));
            }
            reader.close();
//            dg.setColSpecs(cols.toArray(new ColSpec[0]));
            dgConfBuilder.colSpecs((ColSpec[]) cols.toArray());
            // load in mapping files
            for (ColSpec col : cols) {
                if (col.getMapFile() != null) {
                    reader = new BufferedReader(new InputStreamReader(fs.open(new Path(col.getMapFile()))));
                    Map<Integer, Object> map = col.getMap();
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        int key = Integer.parseInt(fields[0]);
                        if (col.getDataType() == DataType.DOUBLE) {
                            map.put(key, Double.parseDouble(fields[1]));
                        } else if (col.getDataType() == DataType.FLOAT) {
                            map.put(key, Float.parseFloat(fields[1]));
                        } else {
                            map.put(key, fields[1]);
                        }
                    }

                    reader.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config file. " + e);
        }
        dgConf = dgConfBuilder.build();
        writer = new Writer(dgConf);
    }

    public void map(LongWritable key, Text value, OutputCollector<String, String> output, Reporter reporter) throws IOException {
        int intialsz = dgConf.getColSpecs().length * 50;

        if (!hasInput) {
            long numRows = Long.parseLong(value.toString().trim());
            // dg.numRows = numRows; //TODO??

            for (int i = 0; i < numRows; i++) {
                StringWriter str = new StringWriter(intialsz);
                PrintWriter pw = new PrintWriter(str);
                writer.writeLine(pw);
                output.collect(null, str.toString());

                if ((i + 1) % 10000 == 0) {
                    reporter.progress();
                    reporter.setStatus("" + (i + 1) + " tuples generated.");
                }
            }
        } else {
            StringWriter str = new StringWriter(intialsz);
            PrintWriter pw = new PrintWriter(str);
            pw.write(value.toString());
            writer.writeLine(pw);
            output.collect(null, str.toString());
        }
    }


}
