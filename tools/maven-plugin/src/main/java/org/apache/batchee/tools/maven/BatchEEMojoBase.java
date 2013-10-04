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
package org.apache.batchee.tools.maven;

import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.jaxrs.client.BatchEEJAXRSClientFactory;
import org.apache.batchee.tools.maven.locator.MavenPluginLocator;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.Map;

public abstract class BatchEEMojoBase extends AbstractMojo {
    /**
     * the BatchEE properties when executed locally
     */
    @Parameter
    protected Map<String, String> properties;

    /**
     * when executed remotely the base url
     */
    @Parameter(property = "batchee.base-url")
    private String baseUrl;

    /**
     * force to use a custom JobOperator
     */
    @Parameter(property = "batchee.job-operator")
    private String jobOperatorClass;

    /**
     * The json provider to use to unmarshall responses in remote mode
     */
    @Parameter(property = "batchee.json-provider", defaultValue = "com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider")
    private String jsonProvider;

    protected volatile JobOperator operator = null;

    protected JobOperator getOrCreateOperator() {
        if (operator == null) {
            synchronized (this) {
                if (operator == null) {
                    if (jobOperatorClass != null) {
                        try {
                            operator = JobOperator.class.cast(Thread.currentThread().getContextClassLoader().loadClass(jobOperatorClass).newInstance());
                        } catch (final Exception e) {
                            throw new IllegalArgumentException("JobOperator " + jobOperatorClass + " can't be used", e);
                        }
                    } else if (baseUrl == null) {
                        configureBatchEE();
                        operator = BatchRuntime.getJobOperator();
                    } else {
                        try {
                            operator = BatchEEJAXRSClientFactory.newClient(baseUrl, Thread.currentThread().getContextClassLoader().loadClass(jsonProvider));
                        } catch (final ClassNotFoundException e) {
                            throw new IllegalArgumentException("JSon provider " + jsonProvider + " can't be found", e);
                        }
                    }
                }
            }
        }
        return operator;
    }

    private void configureBatchEE() {
        try {
            final MavenPluginLocator locator = new MavenPluginLocator();
            locator.init(properties);
            ServicesManager.setServicesManagerLocator(locator);
        } catch (final Throwable th) {
            try {
                Thread.currentThread().getContextClassLoader().loadClass("org.apache.batchee.container.services.ServicesManager");

                getLog().error(th.getMessage(), th);
            } catch (final Throwable ignored) {
                getLog().info("You don't use this plugin with BatchEE so configuration will be ignored");
            }
        }
    }
}
