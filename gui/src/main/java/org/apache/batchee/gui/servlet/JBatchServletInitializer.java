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
package org.apache.batchee.gui.servlet;

import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.util.Set;

public class JBatchServletInitializer implements ServletContainerInitializer {
    private static final String DEFAULT_MAPPING = "/jbatch/*";

    @Override
    public void onStartup(final Set<Class<?>> classes, final ServletContext ctx) throws ServletException {
        String mapping = ctx.getInitParameter("apache.batchee.servlet.mapping");
        if (mapping == null) {
            mapping = DEFAULT_MAPPING;
        } else if (!mapping.endsWith("/*")) { // needed for the controller
            mapping += "/*";
        }

        ctx.addServlet("JBatch Servlet", new JBatchController().mapping(mapping)).addMapping(mapping);
    }
}
