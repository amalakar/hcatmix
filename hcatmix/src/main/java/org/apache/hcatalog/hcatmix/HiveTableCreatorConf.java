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
* Author: malakar
*/
public class HiveTableCreatorConf {
    private final String fileName;
    private final int numMappers;
    private final String outputDir;
    private final String pigScriptDir;
    private final boolean createTable;
    private final boolean generateData;
    private final boolean generatePigScripts;
    private final boolean doEverything;

    public static class Builder {
        private String fileName = null;
        private int numMappers = 0;
        private String outputDir = null;
        private String pigScriptDir = null;
        private boolean createTable = false;
        private boolean generateData = false;
        private boolean generatePigScripts = false;
        private Boolean doEverything = null;

        public Builder fileName(final String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder numMappers(final int numMappers) {
            this.numMappers = numMappers;
            return this;
        }

        public Builder outputDir(final String outputDir) {
            if(StringUtils.isEmpty(outputDir)) {
                throw new IllegalArgumentException("output-dir cannot be null");
            }
            if (outputDir.endsWith("/")) {
                this.outputDir = outputDir + "/";
            } else {
                this.outputDir = outputDir;
            }
            return this;
        }

        public Builder pigScriptDir(final String pigScriptDir) {
            this.pigScriptDir = pigScriptDir;
            return this;
        }

        public Builder createTable(final boolean createTable) {
            this.createTable = createTable;
            return this;
        }

        public Builder generateData(final boolean generateData) {
            this.generateData = generateData;
            return this;
        }

        public Builder generatePigScripts(final boolean generatePigScripts) {
            this.generatePigScripts = generatePigScripts;
            return this;
        }

        public Builder doEverything(final boolean doEverything) {
            this.doEverything = doEverything;
            return this;
        }

        public HiveTableCreatorConf build() {
            if(((generateData || createTable || generatePigScripts) && doEverything != null)) {
                throw new IllegalArgumentException("create-table or generate-data or generate-pig-scripts cannot be set " +
                    " when do-everything is set");
            }

            if(generateData || createTable || generateData ) {
                doEverything = false;
            } else {
                doEverything = true;
            }

            if((generatePigScripts || doEverything) && StringUtils.isEmpty(pigScriptDir)) {
                throw new IllegalArgumentException("pig-script-output-dir cannot be null, when generate-pig-scripts is set");
            }

            if((generateData || doEverything) && StringUtils.isEmpty(outputDir)) {
                throw new IllegalArgumentException("output-dir cannot be null, when generate-data is set");
            }

            return new HiveTableCreatorConf(this);
        }
    }

    HiveTableCreatorConf(Builder builder) {
        this.fileName = builder.fileName;
        this.numMappers = builder.numMappers;
        this.outputDir = builder.outputDir;
        this.pigScriptDir = builder.pigScriptDir;
        this.createTable = builder.createTable;
        this.generateData = builder.generateData;
        this.generatePigScripts = builder.generatePigScripts;
        this.doEverything = builder.doEverything;
    }

    public String getFileName() {
        return fileName;
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

    public boolean isCreateTable() {
        return createTable;
    }

    public boolean isGenerateData() {
        return generateData;
    }

    public boolean isGeneratePigScripts() {
        return generatePigScripts;
    }

    public boolean isDoEverything() {
        return doEverything;
    }
}
