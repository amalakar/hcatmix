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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;
import org.apache.pig.test.utils.DataType;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: malakar
 */
public class PigScriptGenerator {
    public final static String PIG_SCRIPT_TEMPLATE = "input_data = load ''{0}'' USING {1} AS ({2});\n"
                                            + "STORE input_data into ''{3}'' USING  {4};\n";
    public final static String HCAT_LOADER = "org.apache.hcatalog.pig.HCatLoader()";
    public final static String HCAT_STORER = "org.apache.hcatalog.pig.HCatStorer()";
    public final static String PIG_LOADER = "PigStorage('" + HCatMixSetup.SEPARATOR + "')";
    public final static String PIG_STORER = "PigStorage()";

    private final String inputLocation;
    private final String pigOutputLocation;
    private final HiveTableSchema hiveTableSchema;
    private final String pigSchema;

    public PigScriptGenerator(String inputLocation, String pigOutputLocation, HiveTableSchema hiveTableSchema) {
        this.inputLocation = inputLocation;
        this.pigOutputLocation = pigOutputLocation;
        this.hiveTableSchema = hiveTableSchema;
        this.pigSchema = getPigFieldSchema(hiveTableSchema);
    }
    public String getPigLoaderHCatStorerScript() {
        return MessageFormat.format(PIG_SCRIPT_TEMPLATE, inputLocation, PIG_LOADER, pigSchema,
            hiveTableSchema.getName(), HCAT_STORER);
    }

    public String getPigLoaderPigStorerScript() {
        return MessageFormat.format(PIG_SCRIPT_TEMPLATE, inputLocation, PIG_LOADER, pigSchema,
            pigOutputLocation, PIG_STORER);
    }

    public String getHCatLoaderPigStorerScript() {
        return MessageFormat.format(PIG_SCRIPT_TEMPLATE, inputLocation, HCAT_LOADER, pigSchema,
            pigOutputLocation, PIG_STORER);
    }

    public static String getPigFieldSchema(HiveTableSchema hiveTableSchema) {
        List<FieldSchema> allFields = new ArrayList<FieldSchema>();
        allFields.addAll(hiveTableSchema.getColumnFieldSchemas());
        allFields.addAll(hiveTableSchema.getPartitionFieldSchemas());

        StringBuilder fields = new StringBuilder();
        String delim = "";
        for (FieldSchema field : allFields) {
            fields.append(delim).append(field.getName()).append(':').append(toPigType(field.getType()));
            delim = ", ";
        }
        return fields.toString();
    }

    public static String toPigType(String type) {
        DataType dataType = DataType.fromString(type);
        if(dataType == DataType.STRING) {
            return "chararray";
        } else {
            return type;
        }
    }
}
