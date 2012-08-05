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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchemaXMLParser {
    private Document xmlDoc;
    private HiveTableSchemas hiveTableSchemas;

    public TableSchemaXMLParser(String fileName) throws ParserConfigurationException, IOException, SAXException {
        File file = new File(fileName);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        xmlDoc = db.parse(file);
        xmlDoc.getDocumentElement().normalize();
        parse();
    }

    private void parse() {
        final List<MultiInstanceHiveTableSchema> multiInstanceTables = new ArrayList<MultiInstanceHiveTableSchema>();

        NodeList tableList = xmlDoc.getElementsByTagName("tables");
        for (int i = 0; i < tableList.getLength(); i++) {
            Element table = (Element) tableList.item(i);

            MultiInstanceHiveTableSchema multiInstanceSchema = new MultiInstanceHiveTableSchema();
            multiInstanceSchema.setNamePrefix(getElementValue(table, "namePrefix"));

            List<Map<String, String>> columns = getAllChildrensMap(table, "column");
            for (Map<String, String> column : columns) {
                multiInstanceSchema.addColumn(column.get("name"), column.get("type"));
            }

            List<Map<String, String>> partitions = getAllChildrensMap(table, "partition");
            for (Map<String, String> partition : partitions) {
                multiInstanceSchema.addPartition(partition.get("name"), partition.get("type"));
            }

            List<Map<String, String>> instances = getAllChildrensMap(table, "instance");
            for (Map<String, String> instance : instances) {
                multiInstanceSchema.addInstance(instance.get("size"), instance.get("count"));
            }
            multiInstanceTables.add(multiInstanceSchema);
            multiInstanceSchema.setDatabaseName(getElementValue(table, "dbName"));
        }
        hiveTableSchemas =  HiveTableSchemas.fromMultiInstanceSchema(multiInstanceTables);
    }

    private static List<Map<String, String>> getAllChildrensMap(Element element, final String superChildName) {

        Element parent = (Element) element.getElementsByTagName(superChildName + "s").item(0);
        NodeList nodeList = parent.getElementsByTagName(superChildName);

        ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
        if (nodeList != null && nodeList.getLength() > 0)

        {
            for (int i = 0; i < nodeList.getLength(); i++) {
                list.add(getChildrenAsMap(nodeList.item(i)));
            }
        }
        return list;
    }

    private static Map<String, String> getChildrenAsMap(Node node) {
        Map<String, String> childrenAsMap = new HashMap<String, String>();
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                NodeList nl = child.getChildNodes();
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

    public HiveTableSchemas getHiveTableSchemas() {
        return hiveTableSchemas;
    }
}
