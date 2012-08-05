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

package org.apache.hcatalog.hcatmix.conf;

import java.util.*;

public class HiveTableSchemas extends ArrayList<HiveTableSchema>{

    public static HiveTableSchemas fromMultiInstanceSchema(List<MultiInstanceHiveTableSchema> multiInstanceTables) {
        HiveTableSchemas hiveTableSchemas = new HiveTableSchemas();
        for (MultiInstanceHiveTableSchema multiInstanceTable : multiInstanceTables) {
            for (MultiInstanceHiveTableSchema.TableInstance instance  : multiInstanceTable.getInstances()) {
                for (int i = 0; i < instance.getCount(); i++) {
                    String tableName = multiInstanceTable.getNamePrefix() + "_" + instance.getSize() +"_" + i;
                    hiveTableSchemas.add(new HiveTableSchema(multiInstanceTable,  tableName));
                }
            }
        }
        return hiveTableSchemas;
    }
}
