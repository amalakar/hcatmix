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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Author: malakar
 */
public class HiveTableSchemas implements Iterable<HiveTableSchemas.HiveTableSchema>{
    private String databaseName;
    private final List<HiveTableSchemaBase> tableBases = new ArrayList<HiveTableSchemaBase>();
    private final ArrayList<HiveTableSchema> tables = new ArrayList<HiveTableSchema>();

    public HiveTableSchemas(String fileName) throws ConfigurationException, ParserConfigurationException, IOException, SAXException {
        File file = new File(fileName);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(file);
        doc.getDocumentElement().normalize();

        setDatabaseName(getElementValue(doc.getDocumentElement(), "name"));
        NodeList tableList = doc.getElementsByTagName("tables");
        for (int i = 0; i < tableList.getLength(); i++) {
            Element table = (Element) tableList.item(i);

            HiveTableSchemaBase hiveTableSchemaBase = new HiveTableSchemaBase();
            hiveTableSchemaBase.setNamePrefix(getElementValue(table, "namePrefix"));

            List<Map<String, String>> columns = getAllChildrensMap(table, "column");
            for (Map<String, String> column : columns) {
                hiveTableSchemaBase.addColumn(column.get("name"), column.get("type"));
            }

            List<Map<String, String>> partitions = getAllChildrensMap(table, "partition");
            for (Map<String, String> partition : partitions) {
                hiveTableSchemaBase.addPartition(partition.get("name"), partition.get("type"));
            }

            List<Map<String, String>> instances = getAllChildrensMap(table, "instance");
            for (Map<String, String> instance : instances) {
                hiveTableSchemaBase.addInstance(instance.get("size"), instance.get("count"));
            }
            addTable(hiveTableSchemaBase);
        }

        for (HiveTableSchemaBase tableBase : tableBases) {
            for (TableInstance instance  : tableBase.getInstances()) {
                for (int i = 0; i < instance.getCount(); i++) {
                    String tableName = tableBase.getNamePrefix() + "_" + instance.getSize() +"_" + i;
                    tables.add(new HiveTableSchema(tableBase,  tableName));
                }
            }
        }
    }

    public void addTable(HiveTableSchemaBase tableSchemaBase) {
        tableBases.add(tableSchemaBase);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    private static List<Map<String, String>> getAllChildrensMap(Element element, final String superChildName) {
        Element parent = (Element) element.getElementsByTagName(superChildName + "s").item(0);
        NodeList nodeList = parent.getElementsByTagName(superChildName);

        ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
        if (nodeList!=null && nodeList.getLength()>0 ){
            for(int i=0 ; i < nodeList.getLength() ; i++){
                list.add(getChildrenAsMap(nodeList.item(i)));
            }
        }
        return list;
    }

    private static Map<String, String> getChildrenAsMap(Node node){
        Map<String, String> childrenAsMap = new HashMap<String, String>();
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                NodeList nl = (NodeList) child.getChildNodes();
                String value = nl.item(0).getNodeValue();
                childrenAsMap.put(nl.item(0).getParentNode().getNodeName(), value);
            }
        }
        return childrenAsMap;
    }

    private static String getElementValue(Element element, String name) {
        NodeList nlList = element.getElementsByTagName(name).item(0).getChildNodes();
        String value = nlList.item(0).getNodeValue();
        System.out.println(name + " : " + value);
        return value;
    }

    @Override
    public Iterator<HiveTableSchema> iterator() {
        return tables.iterator();
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

    static class HiveTableSchemaBase {
        private String namePrefix;
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
    }


    public static class HiveTableSchema {
        private HiveTableSchemaBase hiveTableSchemaBase;
        final String name;

        public HiveTableSchema(HiveTableSchemaBase hiveTableSchemaBase, final String name) {
            this.hiveTableSchemaBase = hiveTableSchemaBase;
            this.name = name;
        }

        public List<FieldSchema> getPartitions() {
            return hiveTableSchemaBase.getPartitions();
        }

        public List<FieldSchema> getColumns() {
            return hiveTableSchemaBase.getColumns();
        }

        public String getName() {
            return name;
        }
    }

}
