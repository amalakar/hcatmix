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

package org.apache.hcatalog.hcatmix.load.tasks;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hcatalog.hcatmix.load.HadoopLoadGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An abstract class for HCatalog tasks that are to be load tested.
 *
 */
public abstract class HCatLoadTask implements Task {
    // Needs to be ThreadLocal, hiveMetaStoreClient fails if used in multiple threads
    protected static ThreadLocal<HiveMetaStoreClient> hiveClient;
    protected static ThreadLocal<Integer> numErrors;
    protected static final Logger LOG = LoggerFactory.getLogger(Task.class);
    public static final String HIVE_CONF_TOKEN_KEY = "hive.metastore.token.signature";

    protected static final String DB_NAME = "default";
    protected static final String TABLE_NAME = "load_test_table_0_1";

    protected HCatLoadTask() throws IOException {
        numErrors = new ThreadLocal<Integer>(){
            @Override
            protected Integer initialValue() {
                return 0;
            }
        };
    }

    @Override
    public void configure(JobConf jobConf) throws Exception {
        Token token = jobConf.getCredentials().getToken(new Text(HadoopLoadGenerator.METASTORE_TOKEN_KEY));

        try {
            UserGroupInformation.getCurrentUser().addToken(token);
        } catch (IOException e) {
            LOG.info("Error adding token to user", e);
        }
        if (token == null) {
            throw new IllegalArgumentException("Delegation token needs to be set");
        }

        final HiveConf hiveConf = new HiveConf(Task.class);
        hiveConf.set(HIVE_CONF_TOKEN_KEY, HadoopLoadGenerator.METASTORE_TOKEN_SIGNATURE);
        hiveClient = new ThreadLocal<HiveMetaStoreClient>() {
            @Override
            protected HiveMetaStoreClient initialValue() {
                try {
                    return new HiveMetaStoreClient(hiveConf);
                } catch (MetaException e) {
                    throw new RuntimeException("Couldn't create HiveMetaStoreClient", e);
                }
            }
        };
    }

    public void close() {
        try {
            hiveClient.get().close();
        } catch (Exception e) {
            LOG.error("Couldn't close hiveClient, ignored error", e);
        }
    }

    public int getNumErrors() {
        return numErrors.get();
    }

}
