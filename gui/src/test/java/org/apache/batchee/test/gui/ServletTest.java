/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.test.gui;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.DomNode;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.apache.batchee.gui.servlet.JBatchController;
import org.apache.batchee.test.gui.util.CreateSomeJobs;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.webapp30.WebAppDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.batch.runtime.BatchStatus;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static javax.xml.bind.DatatypeConverter.printBase64Binary;
import static org.junit.Assert.assertEquals;

@RunWith(Arquillian.class)
public class ServletTest {
    @ArquillianResource
    private URL base;

    @Test
    public void home() throws IOException {
        assertEquals("init", extractContent("", "/ul/li/a/text()"));
    }

    @Test
    public void instances() throws IOException {
        assertEquals(BatchStatus.COMPLETED.name(), extractContent("executions/" + printBase64Binary("init".getBytes()), "/table/tbody/tr/td[2]"));
    }

    @Test
    public void steps() throws IOException {
        assertEquals("step1", extractContent("step-executions/0", "/table/tbody/tr/td[2]"));
    }

    private String extractContent(final String endUrl, final String xpath) throws IOException {
        final String url = base.toExternalForm() + "jbatch/" + endUrl;
        final WebClient webClient = new WebClient();
        webClient.getOptions().setJavaScriptEnabled(false);
        webClient.getOptions().setCssEnabled(false);
        webClient.getOptions().setAppletEnabled(false);

        final HtmlPage page = webClient.getPage(url);
        final List<?> byXPath = page.getByXPath("//div[@id=\"content\"]" + xpath);
        assertEquals(1, byXPath.size());

        final Object next = byXPath.iterator().next();
        if (!DomNode.class.isInstance(next)) {
            throw new IllegalArgumentException("Can't find text for " + next);
        }
        return DomNode.class.cast(next).asText();
    }

    @Deployment(testable = false)
    public static Archive<?> war() {
        final WebArchive webArchive = ShrinkWrap.create(WebArchive.class, "batchee-gui.war")
            .addAsWebInfResource(new StringAsset(
                Descriptors.create(WebAppDescriptor.class)
                    .metadataComplete(true)
                    .createListener()
                        .listenerClass(CreateSomeJobs.class.getName())
                    .up()
                    .createServlet()
                        .servletName("JBatch")
                        .servletClass(JBatchController.class.getName())
                        .loadOnStartup(1)
                    .up()
                    .createServletMapping()
                        .servletName("JBatch")
                        .urlPattern("/jbatch/*")
                    .up()
                    .exportAsString()), "web.xml")
            // GUI
            .addPackages(true, JBatchController.class.getPackage())
            // test data to create some job things to do this test
            .addPackage(CreateSomeJobs.class.getPackage())
            .addAsWebInfResource("META-INF/batch-jobs/init.xml", "classes/META-INF/batch-jobs/init.xml");

        for (final String resource : Arrays.asList("layout.jsp", "jobs.jsp", "job-instances.jsp", "step-executions.jsp",
                                                    "css/bootstrap.min.3.0.0.css", "js/bootstrap.min.3.0.0.js")) {
            webArchive.addAsWebResource("META-INF/resources/internal/batchee/" + resource, "internal/batchee/" + resource);
        }

        return webArchive;
    }
}
