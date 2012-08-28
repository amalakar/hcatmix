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

public class DataGeneratorConf {
    private final ColSpec[] colSpecs;
    private final long seed;
    private final long numRows;
    private final int numMappers;
    private final String outputFile;
    private final String inFile;
    private final char separator;

    public static class Builder {
        private long seed = -1;
        private long numRows = -1;
        private int numMappers = -1;
        private String outputFile;
        private String inFile;
        private char separator = Writer.CTRL_A;
        ColSpec[] colSpecs;

        public Builder seed(final long seed) {
            this.seed = seed;
            return this;
        }

        public Builder numRows(final long numRows) {
            this.numRows = numRows;
            return this;
        }

        public Builder numMappers(final int numMappers) {
            this.numMappers = numMappers;
            return this;
        }

        public Builder outputFile(final String outputFile) {
            this.outputFile = outputFile;
            return this;
        }

        public Builder inFile(final String inFile) {
            this.inFile = inFile;
            return this;
        }

        public Builder separator(char separator) {
            this.separator = separator;
            return this;
        }

        public Builder colSpecs(ColSpec[] colSpecs) {
            this.colSpecs = colSpecs;
            return this;
        }

        public DataGeneratorConf build() {
            if (numRows < 1 && inFile == null) {
                throw new IllegalArgumentException("With numRows[" + numRows +"] < 1 inFile cannot be null");
            }

            if (numRows > 0 && inFile != null) {
                throw new IllegalArgumentException("inFile[" + inFile + "] should be empty when numRows["
                        + numRows + "] is > 0");
            }

            if (numMappers > 0 && seed != -1) {
                throw new IllegalArgumentException("");
            }
            seed = System.currentTimeMillis();
            return new DataGeneratorConf(this);
        }
    }

    private DataGeneratorConf(Builder builder) {
        this.colSpecs = builder.colSpecs;
        this.seed = builder.seed;
        this.numRows = builder.numRows;
        this.numMappers = builder.numMappers;
        this.outputFile = builder.outputFile;
        this.inFile = builder.inFile;
        this.separator = builder.separator;
    }

    public long getSeed() {
        return seed;
    }

    public long getNumRows() {
        return numRows;
    }

    public int getNumMappers() {
        return numMappers;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getInFile() {
        return inFile;
    }

    public ColSpec[] getColSpecs() {
        return colSpecs;
    }

    public char getSeparator() {
        return separator;
    }
}
