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

import java.util.ArrayList;
import java.util.List;

public class HiveTableSchema {
    private final MultiInstanceHiveTablesSchema multiInstanceHiveTablesSchema;
    private final String name;
    private int rowCount;

    public HiveTableSchema(MultiInstanceHiveTablesSchema multiInstanceHiveTablesSchema, final String name, int rowCount) {
        this.multiInstanceHiveTablesSchema = multiInstanceHiveTablesSchema;
        this.name = name;
        this.rowCount = rowCount;
    }

    public List<MultiInstanceHiveTablesSchema.Column> getPartitions() {
        return multiInstanceHiveTablesSchema.getPartitions();
    }

    // TODO: ColSpec and FieldSchema are stored together, if array of one is required then we need to go through the whole array
    public List<ColSpec> getParitionColSpecs() {
        List<ColSpec> colSpecs = new ArrayList<ColSpec>();
        List<MultiInstanceHiveTablesSchema.Column> paritions = getPartitions();
        for (MultiInstanceHiveTablesSchema.Column partition : paritions) {
            colSpecs.add(partition.getColSpec());
        }
        return  colSpecs;
    }

    public List<FieldSchema> getPartitionFieldSchemas() {
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        List<MultiInstanceHiveTablesSchema.Column> partitions = getPartitions();
        for (MultiInstanceHiveTablesSchema.Column partition : partitions) {
            fieldSchemas.add(partition);
        }
        return fieldSchemas;
    }

    public List<MultiInstanceHiveTablesSchema.Column> getColumns() {
        return multiInstanceHiveTablesSchema.getColumns();
    }

    public List<ColSpec> getColSpecs() {
        List<ColSpec> colSpecs = new ArrayList<ColSpec>();
        List<MultiInstanceHiveTablesSchema.Column> columns = getColumns();
        for (MultiInstanceHiveTablesSchema.Column column : columns) {
            colSpecs.add(column.getColSpec());
        }
        return  colSpecs;
    }

    public List<FieldSchema> getFieldSchemas() {
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        List<MultiInstanceHiveTablesSchema.Column> columns = getColumns();
        for (MultiInstanceHiveTablesSchema.Column column : columns) {
            fieldSchemas.add(column);
        }
        return fieldSchemas;
    }

    public String getName() {
        return name;
    }

    public String getDatabaseName() {
        return multiInstanceHiveTablesSchema.getDatabaseName();
    }

    public int getRowCount() {
        return  rowCount;
    }
}
