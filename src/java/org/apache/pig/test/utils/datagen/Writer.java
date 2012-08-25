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

import java.io.PrintWriter;
import java.util.Random;

/**
 * Author: malakar
 */
public class Writer {
    Random rand;
    private DataGeneratorConf dgConf;
    private String[] mapkey = { "a", "b", "c", "d", "e", "f", "g", "h", "i",
            "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w",
            "x", "y", "z"};
    final public static char CTRL_C = '\u0003';
    final public static char CTRL_A = '\u0001';
    public Writer(DataGeneratorConf dgConf) {
        this.rand = new Random(dgConf.getSeed());
        this.dgConf = dgConf;
    }

    public void writeLine(PrintWriter out) {
        for (int j = 0; j < dgConf.getColSpecs().length; j++) {
            if (j != 0) out.print(dgConf.getSeparator());
            // First, decide if it's going to be null
            if (rand.nextInt(100) < dgConf.getColSpecs()[j].getPercentageNull()) {
                continue;
            }
            writeCol(dgConf.getColSpecs()[j], out);
        }
    }

    public void writeCol(ColSpec colspec, PrintWriter out) {
        switch (colspec.getDataType()) {
            case INT:
                out.print(colspec.nextInt());
                break;

            case LONG:
                out.print(colspec.nextLong());
                break;

            case FLOAT:
                out.print(colspec.nextFloat());
                break;

            case DOUBLE:
                out.print(colspec.nextDouble());
                break;

            case STRING:
                out.print(colspec.nextString());
                break;

            case MAP:
                int len = rand.nextInt(20) + 6;
                for (int k = 0; k < len; k++) {
                    if (k != 0) out.print(CTRL_C);
                    out.print(mapkey[k] + CTRL_C);
                    out.print(colspec.getGen().randomString());
                }
                break;

            case BAG:
                int numElements = rand.nextInt(5) + 5;
                for (int i = 0; i < numElements; i++) {
                    if (i != 0) out.print(CTRL_C);
                    out.print(colspec.getBagColSpec().getDataType().character());
                    writeCol(colspec.getBagColSpec(), out);
                }
        }
    }
}
