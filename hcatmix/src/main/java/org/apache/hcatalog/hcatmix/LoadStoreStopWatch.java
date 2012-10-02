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
import org.perf4j.slf4j.Slf4JStopWatch;


/**
 * Author: malakar
 */
public class LoadStoreStopWatch extends Slf4JStopWatch {
    String hcatSchemaFile;
    LoadStoreType type;

    public enum LoadStoreType {
        PIG_LOAD_HCAT_STORE,
        PIG_LOAD_PIG_STORE,
        HCAT_LOAD_PIG_STORE,
        HCAT_LOAD_HCAT_STORE
    }

    public LoadStoreStopWatch(String hcatSchemaFile, LoadStoreType type) {
        super(hcatSchemaFile + "-" + type.toString());
        this.hcatSchemaFile = hcatSchemaFile;
        this.type = type;
    }

    public static String getTypeFromTag(String tagName) {
        String[] parts = tagName.split("-");
        return parts[1];
    }

    public static String getFileNameFromTag(String tagName) {
        String[] parts = tagName.split("-");
        return parts[0];
    }
}
