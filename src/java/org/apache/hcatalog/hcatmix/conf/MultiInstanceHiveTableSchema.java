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

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.List;

public class MultiInstanceHiveTableSchema {
    private String namePrefix;
    private String databaseName;
    final List<FieldSchema> partitions = new ArrayList<FieldSchema>();
    final List<FieldSchema> columns = new ArrayList<FieldSchema>();
    final List<TableInstance> instances = new ArrayList<TableInstance>();

    public void setNamePrefix(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    public String getNamePrefix() {
        return namePrefix;
    }


    public void addColumn(String name, String type) {
        columns.add(new FieldSchema(name, type, ""));
    }

    public List<FieldSchema> getColumns() {
        return columns;
    }

    public void addPartition(String name, String type) {
        partitions.add(new FieldSchema(name, type, ""));
    }

    public List<FieldSchema> getPartitions() {
        return partitions;
    }

    public void addInstance(String size, String count) {
        instances.add(new TableInstance(size, count));
    }

    public List<TableInstance> getInstances() {
        return instances;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    static class TableInstance {
        private int size;
        private int count;

        public TableInstance(String size, String count) {
            setSize(size);
            setCount(count);
        }

        public int getSize() {
            return size;
        }

        public void setSize(String size) {
            this.size = Integer.parseInt(size);
        }

        public int getCount() {
            return count;
        }

        public void setCount(String count) {
            this.count = Integer.parseInt(count);
        }
    }
}
