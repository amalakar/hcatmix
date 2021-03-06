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

import junit.framework.Assert;
import org.apache.commons.lang.StringUtils;
import org.apache.hcatalog.hcatmix.conf.HiveTableSchema;
import org.apache.hcatalog.hcatmix.conf.TableSchemaXMLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.List;

public class HCatMixUtils {
    public static final String COPY_TABLE_NAME_SUFFIX = "_copy";
    private static final Logger LOG = LoggerFactory.getLogger(HCatMixUtils.class);

    /**
     * The returned location would be a directory in case of map reduce mode, otherwise a file in case of
     * local mode
     * @param outputDir
     * @param tableName
     * @return
     */
    public static String getDataLocation(final String outputDir, final String tableName) {
        return outputDir + tableName;
    }

    /**
     * Get the script file name where PigLoader and HCatStorer is used
     * @param pigScriptDir
     * @param tableName
     * @return
     */
    public static String getHCatStoreScriptName(final String pigScriptDir, final String tableName) {
        return appendSlashIfRequired(pigScriptDir) + tableName + ".pigLoadHcatStore.pig";
    }

    /**
     * Get the script file name where HCatStorer and PigLoader is used
     * @param pigScriptDir
     * @param tableName
     * @return
     */
    public static String getHCatLoadScriptName(final String pigScriptDir, final String tableName) {
        return appendSlashIfRequired(pigScriptDir) + tableName + ".hcatLoadPigStore.pig";
    }

    /**
     * Get the script file name where the default pig PigLoader() and PigStorer() is used
     * @param pigScriptDir
     * @param tableName
     * @return
     */
    public static String getPigLoadStoreScriptName(final String pigScriptDir, final String tableName) {
        return appendSlashIfRequired(pigScriptDir) + tableName + ".pigLoadPigStore.pig";
    }

    /**
     * Get the script file name where HCatLoader() and HCatStorer() is used
     * @param pigScriptDir
     * @param tableName
     * @return
     */
    public static String getHCatLoadStoreScriptName(final String pigScriptDir, final String tableName) {
        return appendSlashIfRequired(pigScriptDir) + tableName + ".hcatLoadHCatStore.pig";
    }

    /**
     * Check that the directory name is valid and append a slash to it if required
     * @param outputDir
     * @return
     */
    public static String appendSlashIfRequired(String outputDir) {
        if(StringUtils.isEmpty(outputDir)) {
            throw new IllegalArgumentException("The directory name cannot be null/empty");
        }
        if (!outputDir.endsWith("/")) {
            outputDir += "/";
        }
        return outputDir;
    }

    public static String getCopyTableName(String tableName) {
        return tableName + COPY_TABLE_NAME_SUFFIX;
    }

    public static String removeCopyFromTableName(String tableName) {
        if(tableName == null || !tableName.endsWith(COPY_TABLE_NAME_SUFFIX)) {
            throw new IllegalArgumentException(COPY_TABLE_NAME_SUFFIX + " suffix could only be removed if present in the name:"
                + tableName);
        }
        return tableName.substring(0, tableName.lastIndexOf(COPY_TABLE_NAME_SUFFIX));
    }

    public static String getPigOutputLocation(final String pigOutputRoot, final String dbName, final String tableName) {
        return HCatMixUtils.appendSlashIfRequired(pigOutputRoot) + dbName + "." + tableName + "/";
    }

    public static void assertDirExists(String dirName) {
        File directory = new File(dirName);
        if(!directory.exists()) {
            throw new IllegalStateException("The directory name: " + dirName + " does not exist");
        }

        if(!directory.isDirectory()) {
            throw new IllegalStateException("The directory name: " + dirName + " is not a directory");
        }
    }

    public static void logAndThrow(RuntimeException e) {
        LOG.error(e.getMessage(), e);
        throw e;
    }

    /**
     * Look for the file in file system first, if unavailable look for the same in the classpath.
     *
     * @param fileName the file to look for (first in path then in classpath)
     * @return the input stream for the given file
     * @throws FileNotFoundException
     */
    public static InputStream getInputStream(String fileName) throws FileNotFoundException {
        InputStream is;
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            is = new FileInputStream(file);
            LOG.info(fileName + " found in file system path will use it, wont look in classpath");
        } else {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            if (is == null) {
                throw new FileNotFoundException("Couldn't find " + fileName + " in classpath. Aborting");
            }
            LOG.info(fileName + " found in classpath, will use it.");
        }
        return is;
    }

    /**
     * Returnt the first table schema from a spec file
     * @param hcatTableSpecFile
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public static HiveTableSchema getFirstTableFromConf(String hcatTableSpecFile) throws IOException, SAXException, ParserConfigurationException {
        TableSchemaXMLParser configParser = new TableSchemaXMLParser(getInputStream(hcatTableSpecFile));
        List<HiveTableSchema> multiInstanceList = configParser.getHiveTableList();
        return  multiInstanceList.get(0);
    }

    /**
     *
     * @return temporary directory
     */
    public static String getTempDirName() {
        String tmpDir = System.getProperty("buildDirectory");
        if( tmpDir == null) {
            if(new File("target/").exists()) {
                tmpDir = "target";
            } else {
                tmpDir = "/tmp";
            }
        }
        return tmpDir;
    }
}
