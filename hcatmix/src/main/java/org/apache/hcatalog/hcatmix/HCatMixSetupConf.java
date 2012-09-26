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

package org.apache.hcatalog.hcatmix;

import org.apache.commons.lang.StringUtils;

/**
 * The configuration class for configuring how to create table/generate data etc.
 */
public class HCatMixSetupConf {
    private final String confFileName;
    private final int numMappers;
    private final String outputDir;
    private final String pigScriptDir;
    private final boolean createTable;
    private final boolean generateData;
    private final boolean generatePigScripts;
    private final String pigDataOutputDir;


    public static class Builder {
        private String confFileName = null;
        private int numMappers = 0;
        private String outputDir = null;
        private String pigScriptDir = null;
        private String pigDataOutputDir = null;
        private boolean createTable = false;
        private boolean generateData = false;
        private boolean generatePigScripts = false;
        private Boolean doEverything = null;

        public Builder confFileName(final String fileName) {
            this.confFileName = fileName;
            return this;
        }

        public Builder numMappers(final int numMappers) {
            this.numMappers = numMappers;
            return this;
        }

        /**
         * Directory where data is generated (could be local/HDFS based on the mode)
         * @param outputDir
         * @return
         */
        public Builder outputDir(final String outputDir) {
            try {
                this.outputDir = HCatMixUtils.appendSlashIfRequired(outputDir);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Output directory cannot be null/empty", e);
            }
            return this;
        }

        /**
         * The local file system directory to store generated pig scripts
         * @param pigScriptDir
         * @return
         */
        public Builder pigScriptDir(final String pigScriptDir) {
            try {
                this.pigScriptDir = HCatMixUtils.appendSlashIfRequired(pigScriptDir);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Pig script directory cannot be null/empty", e);
            }
            return this;
        }

        public Builder createTable() {
            this.createTable = true;
            return this;
        }

        public Builder generateData() {
            this.generateData = true;
            return this;
        }

        public Builder generatePigScripts() {
            this.generatePigScripts = true;
            return this;
        }

        /**
         * The directory location where to store data that is written using default PigStorer()
         * (could be local/HDFS based on the mode)
         * @param pigDataOutputDir
         * @return
         */
        public Builder pigDataOutputDir(String pigDataOutputDir) {
            try {
                this.pigDataOutputDir = HCatMixUtils.appendSlashIfRequired(pigDataOutputDir);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Pig data output directory cannot be null/empty", e);
            }
            return this;
        }

        public Builder doEverything() {
            this.doEverything = true;
            return this;
        }

        public HCatMixSetupConf build() {
            if(((generateData || createTable || generatePigScripts) && doEverything != null)) {
                throw new IllegalArgumentException("Special switch for creating table, generating data and for generating "
                    + "pig scripts cannot be set when do-everything is set");
            }

            if(generateData || createTable || generateData ) {
                doEverything = false;
            } else {
                // If none of the special switch is set, default behaviour is to do everything
                doEverything = true;
            }

            if(doEverything) {
                generateData = true;
                generatePigScripts = true;
                createTable = true;
            }

            if((generateData || generatePigScripts) && StringUtils.isEmpty(outputDir)) {
                throw new IllegalArgumentException("Output directory name cannot be null/empty, when data/pig script is to be generated");
            }

            if((generatePigScripts) && StringUtils.isEmpty(pigScriptDir)) {
                throw new IllegalArgumentException("Pig script output directory name cannot be null/empty, when pig script is to be generated");
            }

            if((generatePigScripts) && StringUtils.isEmpty(pigDataOutputDir)) {
                throw new IllegalArgumentException("Pig data output directory name cannot be null/empty, when pig script is to be generated");
            }

            if(StringUtils.isEmpty(confFileName)) {
                throw new IllegalArgumentException("Conf file name cannot be null/empty");
            }

            return new HCatMixSetupConf(this);
        }
    }

    HCatMixSetupConf(Builder builder) {
        this.confFileName = builder.confFileName;
        this.numMappers = builder.numMappers;
        this.outputDir = builder.outputDir;
        this.pigScriptDir = builder.pigScriptDir;
        this.createTable = builder.createTable;
        this.generateData = builder.generateData;
        this.generatePigScripts = builder.generatePigScripts;
        this.pigDataOutputDir = builder.pigDataOutputDir;
    }

    public String getConfFileName() {
        return confFileName;
    }

    public int getNumMappers() {
        return numMappers;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public String getPigScriptDir() {
        return pigScriptDir;
    }

    public String getPigDataOutputDir() {
        return pigDataOutputDir;
    }

    public boolean isCreateTable() {
        return createTable;
    }

    public boolean isGenerateData() {
        return generateData;
    }

    public boolean isGeneratePigScripts() {
        return generatePigScripts;
    }
}
