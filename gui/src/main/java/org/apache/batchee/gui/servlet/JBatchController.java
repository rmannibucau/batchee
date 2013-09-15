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
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JBatchController extends HttpServlet {
    private static final String DEFAULT_MAPPING_SERVLET25 = "/jbatch";

    private static final int EXECUTION_BY_PAGE = 30;

    private JobOperator operator;
    private String mapping;
    private String executionMapping;
    private String context;
    private String stepExecutionMapping;

    public JBatchController mapping(final String rawMapping) {
        this.mapping = rawMapping.substring(0, rawMapping.length() - 2); // mapping pattern is /xxx/*
        return this;
    }

    @Override
    public void init(final ServletConfig config) throws ServletException {
        this.operator = BatchRuntime.getJobOperator();

        this.context = config.getServletContext().getContextPath();
        if ("/".equals(context)) {
            context = "";
        }

        if (mapping == null) { // used directly with servlet 3.0
            mapping = DEFAULT_MAPPING_SERVLET25;
        }
        mapping = context + mapping;

        // prepare mappings to ease matching
        executionMapping = mapping + "/executions/";
        stepExecutionMapping = mapping + "/step-executions/";
    }

    @Override
    protected void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        resp.setCharacterEncoding("UTF-8");
        req.setAttribute("context", context);
        req.setAttribute("mapping", mapping);

        final String requestURI = req.getRequestURI();
        if (requestURI.startsWith(executionMapping)) {
            final String name = new String(DatatypeConverter.parseBase64Binary(requestURI.substring(executionMapping.length())));
            final int start = extractInt(req, "start");
            listExecutions(req, name, start);
        } else if (requestURI.startsWith(stepExecutionMapping)) {
            final int executionId = Integer.parseInt(requestURI.substring(stepExecutionMapping.length()));
            listStepExecutions(req, executionId);
        } else {
            listJobs(req);
        }

        req.getRequestDispatcher("/internal/batchee/layout.jsp").forward(req, resp);
    }

    private void listStepExecutions(final HttpServletRequest req, final int executionId) {
        final List<StepExecution> steps = operator.getStepExecutions(executionId);
        req.setAttribute("view", "step-executions");
        req.setAttribute("steps", steps);
        req.setAttribute("executionId", executionId);
        req.setAttribute("name", operator.getJobExecution(executionId).getJobName());
    }

    private void listExecutions(final HttpServletRequest req, final String name, final int start) {
        final List<JobInstance> instances = operator.getJobInstances(name, start, EXECUTION_BY_PAGE);
        final Map<JobInstance, List<JobExecution>> executions = new HashMap<JobInstance, List<JobExecution>>();
        for (final JobInstance instance : instances) {
            executions.put(instance, operator.getJobExecutions(instance));
        }

        req.setAttribute("view", "job-instances");
        req.setAttribute("name", name);
        req.setAttribute("executions", executions);

        int nextStart = start + EXECUTION_BY_PAGE;
        if (nextStart > operator.getJobInstanceCount(name)) {
            nextStart = -1;
        }
        req.setAttribute("nextStart", nextStart);
    }

    private void listJobs(final HttpServletRequest req) throws ServletException, IOException {
        Set<String> names = operator.getJobNames();
        if (names == null) {
            names = Collections.emptySet();
        }

        req.setAttribute("view", "jobs");
        req.setAttribute("names", names);
    }

    private static int extractInt(final HttpServletRequest req, final String name) {
        final String string = req.getParameter(name);
        if (string != null) {
            return Integer.parseInt(string);
        }
        return 0;
    }
}
