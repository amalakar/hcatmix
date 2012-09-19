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

import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;

public class HCatMixUtils {
    /**
     * The returned location would be a directory in case of map reduce mode, otherwise a file in case of
     * local mode
     * @param outputDir
     * @param hiveTableSchema
     * @return
     */
    public static String getDataLocation(final String outputDir, final HiveTableSchema hiveTableSchema) {
        return outputDir + hiveTableSchema.getName();
    }

    public static String getPigLoadScriptName(final String pigScriptDir, final String tableName) {
        return pigScriptDir + tableName + ".load.pig";
    }
}
