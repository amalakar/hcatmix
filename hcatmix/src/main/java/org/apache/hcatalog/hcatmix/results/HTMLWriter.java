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

package org.apache.hcatalog.hcatmix.results;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.Properties;

class HTMLWriter {

    private static final Logger LOG = LoggerFactory.getLogger(HTMLWriter.class);
    private static final String TEMPLATE_FILE = "graphs.vm";
    private static final String HTML_FILE = "hcatmix_results.html";

    public static void publish(List<HCatStats> stats) throws Exception {

        try {
            VelocityContext context = new VelocityContext();

            context.put("hcatStats", stats);

            Properties props = new Properties();
            props.setProperty("resource.loader", "file, class, jar");
            props.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
            props.setProperty("runtime.references.strict", "true");

            Velocity.init(props);
            Template template = null;

            try {
                template = Velocity.getTemplate(TEMPLATE_FILE);
            } catch (ResourceNotFoundException e) {
                LOG.error("Cannot publish HTML page. Couldn't find template: " + TEMPLATE_FILE, e);
                throw e;
            } catch (ParseErrorException e) {
                LOG.error("Couldn't publish HTML page. Syntax error in template " + TEMPLATE_FILE, e);
                throw e;
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(HTML_FILE));

            if (template != null)
                template.merge(context, writer);

            writer.flush();
            writer.close();
        } catch (Exception e) {
            LOG.error("Couldn't publish HTML page", e);
            throw e;
        }
    }
}
