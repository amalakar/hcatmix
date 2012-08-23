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

import junit.framework.TestCase;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hcatalog.hcatmix.HiveTableCreator;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class TestHiveTableConf extends TestCase {
    public void testHiveTableConf() throws IOException, SAXException, ConfigurationException, ParserConfigurationException, MetaException {
        TableSchemaXMLParser configParser = new TableSchemaXMLParser("pigmix/scripts/hcat_table_specification.xml");
        HiveTableSchemas schemas = configParser.getHiveTableSchemas();
        HiveTableCreator tableCreator = new HiveTableCreator();
        tableCreator.createTables(schemas);
    }
}
