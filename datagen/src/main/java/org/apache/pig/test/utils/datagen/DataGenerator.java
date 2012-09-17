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

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.tools.cmdline.CmdLineParser;

/**
 * A tool to generate data for performance testing.
 */
public class DataGenerator extends Configured implements Tool {

    public DataGenerator() {
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new DataGenerator(), args);
    }

    @Override
    public int run(String[] args) throws Exception {    	
        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('e', "seed", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('r', "rows", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('s', "separator", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('i', "input", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('m', "mappers", CmdLineParser.ValueExpected.OPTIONAL);

        DataGeneratorConf.Builder builder = new DataGeneratorConf.Builder();

        char opt;
        try {
            while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
                switch (opt) {
                case 'e':
                    builder.seed(Long.valueOf(opts.getValStr()));
                    break;

                case 'f':
                    builder.outputFile(opts.getValStr());
                    break;

                case 'i':
                    builder.inFile(opts.getValStr());
                    break;

                case 'r':
                    builder.numRows(Long.valueOf(opts.getValStr()));
                    break;
    
                case 's':
                    builder.separator(opts.getValStr().charAt(0));
                    break;
                    
                case 'm':
                	builder.numMappers(Integer.valueOf(opts.getValStr()));
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

        String remainders[] = opts.getRemainingArgs();
        ColSpec[] colSpecs = new ColSpec[remainders.length];
        for (int i = 0; i < remainders.length; i++) {
            colSpecs[i] = ColSpec.fromString(remainders[i]);
        }
        builder.colSpecs(colSpecs);
        DataGeneratorConf dgConf = null;
        try {
            dgConf = builder.build();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            usage();
        }
        runJob(dgConf, getConf());
        return 0;
    }

    public void runJob(DataGeneratorConf dgConf, Configuration conf) throws IOException {
    	long t1 = System.currentTimeMillis();
    	if (dgConf.getNumMappers() <= 0) {
    		System.out.println("Generate data in local mode.");
            LocalRunner runner = new LocalRunner(dgConf);

            runner.generate();
        }else{
        	System.out.println("Generate data in hadoop mode.");        
        	HadoopRunner runner = new HadoopRunner(dgConf, conf);
        	runner.generate();
        }
    	   	
    	long t2 = System.currentTimeMillis();
    	System.out.println("Job is successful! It took " + (t2-t1)/1000 + " seconds.");
    }
    
    private static void usage() {
        System.err.println("Usage: datagen -rows numrows [options] colspec ...");
        System.err.println("\tOptions:");
        System.err.println("\t-e -seed seed value for random numbers");
        System.err.println("\t-f -file output file, default is stdout");
        System.err.println("\t-i -input input file, lines will be read from");
        System.err.println("\t\tthe file and additional columns appended.");
        System.err.println("\t\tMutually exclusive with -r.");
        System.err.println("\t-r -rows number of rows to output");
        System.err.println("\t-s -separator character, default is ^A");
        System.err.println("\t-m -number of mappers to run concurrently to generate data. " +
        		"If not specified, DataGenerator runs locally. This option can NOT be used with -e.");
        System.err.println();
        System.err.print("\tcolspec: columntype:average_size:cardinality:");
        System.err.println("distribution_type:percent_null");
        System.err.println("\tcolumntype:");
        System.err.println("\t\ti = int");
        System.err.println("\t\tl = long");
        System.err.println("\t\tf = float");
        System.err.println("\t\td = double");
        System.err.println("\t\ts = string");
        System.err.println("\t\tm = map");
        System.err.println("\t\tbx = bag of x, where x is a columntype");
        System.err.println("\tdistribution_type:");
        System.err.println("\t\tu = uniform");
        System.err.println("\t\tz = zipf");

        throw new RuntimeException();
    }
}