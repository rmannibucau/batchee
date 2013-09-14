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

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.apache.batchee.gui.servlet.Templates.html;

public class JBatchServlet extends HttpServlet {
    private JobOperator operator;

    @Override
    public void init(final ServletConfig config) throws ServletException {
        super.init(config);
        operator = BatchRuntime.getJobOperator();
        Templates.init(config.getInitParameter("mapping"));
    }

    @Override
    protected void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        resp.setCharacterEncoding("UTF-8");

        listJobs(resp);
    }

    private void listJobs(final HttpServletResponse resp) throws IOException {
        Set<String> names = operator.getJobNames();
        if (names == null) {
            names = Collections.emptySet();
        }

        html(resp.getWriter(), "jobs.vm", new MapBuilder.Simple().set("names", names).build());
    }
}
