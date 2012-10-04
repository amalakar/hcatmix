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
import org.apache.velocity.tools.generic.NumberTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.Properties;

class ResultsPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(ResultsPublisher.class);
    private static final String HTML_TEMPLATE = "html_template.vm";
    private static final String HTML_FILE = "hcatmix_results.html";
    private static final String JSON_TEMPLATE = "json_template.vm";
    private static final String JSON_FILE = "hcatmix_results.json";
    private final VelocityContext context;

    ResultsPublisher(List<HCatStats> stats) throws Exception {
        context = new VelocityContext();
        context.put("hcatStats", stats);
        context.put("numberTool", new NumberTool());
        Properties props = new Properties();
        props.setProperty("resource.loader", "file, class, jar");
        props.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        props.setProperty("runtime.references.strict", "true");
        try {
            Velocity.init(props);
        } catch (Exception e) {
            LOG.error("Couldn't set properties for velocity", e);
            throw e;
        }
    }

    public void publishAll() throws Exception {
        publishUsingTemplate(HTML_TEMPLATE, HTML_FILE);
        publishUsingTemplate(JSON_TEMPLATE, JSON_FILE);

    }

    public void publishUsingTemplate(final String templateFile, final String outputFile) throws Exception {
        try {
            Template template = Velocity.getTemplate(templateFile);
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
            if (template != null) {
                template.merge(context, writer);
            } else {
                LOG.error(templateFile + " is found to be null");
            }
            writer.flush();
            writer.close();
        } catch (ResourceNotFoundException e) {
            LOG.error("Cannot publish HTML page. Couldn't find template: " + templateFile, e);
            throw e;
        } catch (ParseErrorException e) {
            LOG.error("Couldn't publish HTML page. Syntax error in template " + templateFile, e);
            throw e;
        } catch (Exception e) {
            LOG.error("Error loading the template:" + templateFile, e);
            throw e;
        }
    }

}
