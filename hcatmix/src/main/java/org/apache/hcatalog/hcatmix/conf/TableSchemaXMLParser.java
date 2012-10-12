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

import org.apache.hcatalog.hcatmix.HCatMixUtils;
import org.apache.pig.test.utils.DataType;
import org.apache.pig.test.utils.datagen.ColSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchemaXMLParser {
    private static final Logger LOG = LoggerFactory.getLogger(TableSchemaXMLParser.class);

    private final List<HiveTableSchema> hiveTableList;
    public static final String PERCENTAGE_NULL = "percentageNull";
    public static final String CARDINALITY = "cardinality";
    public static final String AVG_LENGTH = "avgLength";
    public static final String DISTRIBUTION = "distribution";
    public static final String TYPE = "type";

    public TableSchemaXMLParser(String fileName) throws ParserConfigurationException, IOException, SAXException {
        this(HCatMixUtils.getInputStream(fileName));
    }

    public TableSchemaXMLParser(InputStream inputStream) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document xmlDoc = db.parse(inputStream);
        xmlDoc.getDocumentElement().normalize();
        hiveTableList = parse(xmlDoc);
    }

    private MultiInstanceHiveTableList parse(Document doc) {
        final List<MultiInstanceHiveTablesSchema> multiInstanceTablesList = new ArrayList<MultiInstanceHiveTablesSchema>();

        Element tables = (Element) doc.getElementsByTagName("tables").item(0);
        NodeList tableList = tables.getElementsByTagName("table");

        for (int i = 0; i < tableList.getLength(); i++) {
            Element table = (Element) tableList.item(i);

            MultiInstanceHiveTablesSchema multiInstanceSchema = new MultiInstanceHiveTablesSchema();
            multiInstanceSchema.setNamePrefix(getElementValue(table, "namePrefix"));

            List<Map<String, String>> columns = getAllChildrensMap(table, "column");
            for (Map<String, String> column : columns) {
                multiInstanceSchema.addColumn(column.get("name"), getColSpecFromMap(column, false));
            }

            if(table.getElementsByTagName("partitions").getLength() > 0) {
                // Partition is optional
                List<Map<String, String>> partitions = getAllChildrensMap(table, "partition");
                for (Map<String, String> partition : partitions) {
                    multiInstanceSchema.addPartition(partition.get("name"), getColSpecFromMap(partition, true));
                }
            }
            List<Map<String, String>> instances = getAllChildrensMap(table, "instance");
            for (Map<String, String> instance : instances) {
                multiInstanceSchema.addInstance(instance.get("size"), instance.get("count"));
            }
            multiInstanceTablesList.add(multiInstanceSchema);
            multiInstanceSchema.setDatabaseName(getElementValue(table, "dbName"));
        }
        return  MultiInstanceHiveTableList.fromMultiInstanceSchema(multiInstanceTablesList);
    }

    private static ColSpec getColSpecFromMap(Map<String, String> column, boolean isPartition) {
        ColSpec.Builder builder = new ColSpec.Builder();
        builder.dataType(DataType.fromString(column.get(TYPE)));
        if(DataType.fromString(column.get(TYPE)) == DataType.STRING) {
            builder.avgStrLength(Integer.parseInt(column.get(AVG_LENGTH)));
        }
        builder
                .cardinality(Integer.parseInt(column.get(CARDINALITY)))
                .distributionType(ColSpec.DistributionType.fromString(column.get(DISTRIBUTION)));
        if(isPartition) {
            if(column.containsKey(PERCENTAGE_NULL)) {
                throw new IllegalArgumentException(PERCENTAGE_NULL + " cannot be defined for a partition. Partition cannot have null values");
            }
        } else {
            builder.percentageNull(Integer.parseInt(column.get("percentageNull")));
        }
        return builder.build();
    }

    private static List<Map<String, String>> getAllChildrensMap(Element element, final String superChildName) {

        Element parent = (Element) element.getElementsByTagName(superChildName + "s").item(0);
        NodeList nodeList = parent.getElementsByTagName(superChildName);

        ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
        if (nodeList != null && nodeList.getLength() > 0) {
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
        LOG.debug(name + " : " + value);
        return value;
    }

    public List<HiveTableSchema> getHiveTableList() {
        return hiveTableList;
    }
}
