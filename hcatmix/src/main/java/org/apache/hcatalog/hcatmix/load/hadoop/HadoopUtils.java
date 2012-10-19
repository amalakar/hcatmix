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

package org.apache.hcatalog.hcatmix.load.hadoop;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Hadoop utitlity class
 */
public class HadoopUtils {

    protected static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

    /**
     * Walks through the classpath and uploads the files in HDFS and also adds them to the JobConf object.
     * The upload files are marked for deletion upon exit. So they gets deleted when the job finishes
     *
     * @param jobConf
     * @param tmpDir  where all the files would be copied to
     * @throws IOException
     */
    public static void uploadClasspathAndAddToJobConf(JobConf jobConf, Path tmpDir) throws IOException {
        final String[] jars = findFilesInCP(new String[]{
                System.getenv().get("CLASSPATH"),
                System.getProperty("java.class.path")
        });
        final FileSystem fs = FileSystem.get(jobConf);
        for (String jarFile : jars) {
            Path srcJarFilePath = new Path("file:///" + jarFile);
            String filename = srcJarFilePath.getName();
            Path tmpJarFilePath = getTmpFileName(tmpDir, filename);
            fs.deleteOnExit(tmpJarFilePath);
            fs.copyFromLocalFile(srcJarFilePath, tmpJarFilePath);
            DistributedCache.addFileToClassPath(tmpJarFilePath, jobConf);
        }
        DistributedCache.createSymlink(jobConf);
    }

    protected static String[] findFilesInCP(String[] classPathLines) {
        Set<String> jars = new HashSet<String>();
        for (String locationsLine : classPathLines) {
            if (locationsLine == null) {
                continue;
            }
            for (String CPentry : locationsLine.split(":")) {
                jars.add(new File(CPentry).getAbsoluteFile().toString());
            }
        }
        return jars.toArray(new String[0]);
    }

    protected static Path getTmpFileName(final Path tmpDir, final String filename) throws IOException {
        return new Path(tmpDir, filename);
    }
}
