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

import java.awt.image.AreaAveragingScaleFilter;
import java.util.ArrayList;
import java.util.List;

public class HiveTableSchema {
    private final MultiInstanceHiveTableSchema multiInstanceHiveTableSchema;
    private final String name;

    public HiveTableSchema(MultiInstanceHiveTableSchema multiInstanceHiveTableSchema, final String name) {
        this.multiInstanceHiveTableSchema = multiInstanceHiveTableSchema;
        this.name = name;
    }

    public List<MultiInstanceHiveTableSchema.Column> getPartitions() {
        return multiInstanceHiveTableSchema.getPartitions();
    }

    // TODO: ColSpec and FieldSchema are stored together, if array of one is required then we need to go through the whole array
    public List<ColSpec> getParitionColSpecs() {
        List<ColSpec> colSpecs = new ArrayList<ColSpec>();
        List<MultiInstanceHiveTableSchema.Column> paritions = getPartitions();
        for (MultiInstanceHiveTableSchema.Column partition : paritions) {
            colSpecs.add(partition.getColSpec());
        }
        return  colSpecs;
    }

    public List<FieldSchema> getPartitionFieldSchemas() {
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        List<MultiInstanceHiveTableSchema.Column> partitions = getPartitions();
        for (MultiInstanceHiveTableSchema.Column partition : partitions) {
            fieldSchemas.add(partition);
        }
        return fieldSchemas;
    }

    public List<MultiInstanceHiveTableSchema.Column> getColumns() {
        return multiInstanceHiveTableSchema.getColumns();
    }

    public List<ColSpec> getColSpecs() {
        List<ColSpec> colSpecs = new ArrayList<ColSpec>();
        List<MultiInstanceHiveTableSchema.Column> columns = getColumns();
        for (MultiInstanceHiveTableSchema.Column column : columns) {
            colSpecs.add(column.getColSpec());
        }
        return  colSpecs;
    }

    public List<FieldSchema> getFieldSchemas() {
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        List<MultiInstanceHiveTableSchema.Column> columns = getColumns();
        for (MultiInstanceHiveTableSchema.Column column : columns) {
            fieldSchemas.add(column);
        }
        return fieldSchemas;
    }

    public String getName() {
        return name;
    }
    public String getDatabaseName() {
        return multiInstanceHiveTableSchema.getDatabaseName();
    }
}
