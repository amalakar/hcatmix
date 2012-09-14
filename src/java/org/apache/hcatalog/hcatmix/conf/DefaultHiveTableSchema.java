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
import org.apache.pig.test.utils.datagen.ColSpec;

import java.util.List;

/**
 * Author: malakar
 */
public class DefaultHiveTableSchema implements HiveTableSchema{
    private List<ColSpec> partitionColSpecs;
    private List<FieldSchema> partitionFieldSchemas;
    private List<ColSpec> columnColSpecs;
    private List<FieldSchema> columnFieldSchemas;
    private String name;
    private String databaseName;
    private int rowCount;


    @Override
    public List<ColSpec> getPartitionColSpecs() {
        return partitionColSpecs;
    }

    public void setPartitionColSpecs(List<ColSpec> partitionColSpecs) {
        this.partitionColSpecs = partitionColSpecs;
    }

    @Override
    public List<FieldSchema> getPartitionFieldSchemas() {
        return partitionFieldSchemas;
    }

    public void setPartitionFieldSchemas(List<FieldSchema> partitionFieldSchemas) {
        this.partitionFieldSchemas = partitionFieldSchemas;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public List<FieldSchema> getColumnFieldSchemas() {
        return columnFieldSchemas;
    }

    public void setColumnFieldSchemas(List<FieldSchema> columnFieldSchemas) {
        this.columnFieldSchemas = columnFieldSchemas;
    }

    @Override
    public List<ColSpec> getColumnColSpecs() {
        return columnColSpecs;
    }

    public void setColumnColSpecs(List<ColSpec> columnColSpecs) {
        this.columnColSpecs = columnColSpecs;
    }
}
