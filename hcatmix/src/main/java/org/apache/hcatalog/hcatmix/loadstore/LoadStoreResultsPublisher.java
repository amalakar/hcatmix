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

package org.apache.hcatalog.hcatmix.loadstore;

import org.apache.hcatalog.hcatmix.publisher.HCatMixFormatter;
import org.apache.hcatalog.hcatmix.publisher.ResultsPublisher;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.tools.generic.DateTool;

import java.util.List;

class LoadStoreResultsPublisher extends ResultsPublisher {
    private static final String HTML_TEMPLATE = "loadstore_html_template.vm";
    private static final String JSON_TEMPLATE = "loadstore_json_template.vm";
    private final String htmlOutFileName;
    private final String jsonOutFileName;

    public LoadStoreResultsPublisher(List<LoadStoreTestStatistics> allStats, final String htmlOutFileName, final String jsonOutFileName) throws Exception {
        super();
        this.htmlOutFileName = htmlOutFileName;
        this.jsonOutFileName = jsonOutFileName;

        VelocityContext context  = new VelocityContext();
        context.put("hcatStats", allStats);
        context.put("formatter", new HCatMixFormatter());
        context.put("dateTool", new DateTool());
        super.setContext(context);
    }

    public void publishAll() throws Exception {
        publishUsingTemplate(HTML_TEMPLATE, htmlOutFileName);
        publishUsingTemplate(JSON_TEMPLATE, jsonOutFileName);
    }
}
