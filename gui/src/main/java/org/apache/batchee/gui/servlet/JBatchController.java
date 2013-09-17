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
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JBatchController extends HttpServlet {
    private static final String DEFAULT_MAPPING_SERVLET25 = "/jbatch";

    private static final int EXECUTION_BY_PAGE = 30;

    private JobOperator operator;
    private String mapping;
    private String executionMapping;
    private String context;
    private String stepExecutionMapping;
    private String startMapping;
    private String doStartMapping;

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
        startMapping = mapping + "/start/";
        doStartMapping = mapping + "/doStart/";
    }

    @Override
    protected void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        resp.setCharacterEncoding("UTF-8");
        req.setAttribute("context", context);
        req.setAttribute("mapping", mapping);

        final String requestURI = req.getRequestURI();
        if (requestURI.startsWith(executionMapping)) {
            final String name = URLDecoder.decode(requestURI.substring(executionMapping.length()), "UTF-8");
            final int start = extractInt(req, "start", -1);
            listExecutions(req, name, EXECUTION_BY_PAGE, start);
        } else if (requestURI.startsWith(stepExecutionMapping)) {
            final int executionId = Integer.parseInt(requestURI.substring(stepExecutionMapping.length()));
            listStepExecutions(req, executionId);
        } else if (requestURI.startsWith(startMapping)) {
            final String name = URLDecoder.decode(requestURI.substring(startMapping.length()), "UTF-8");
            start(req, name);
        } else if (requestURI.startsWith(doStartMapping)) {
            final String name = URLDecoder.decode(requestURI.substring(doStartMapping.length()), "UTF-8");
            final Properties properties = readProperties(req);
            doStart(req, name, properties);
        } else {
            listJobs(req);
        }

        req.getRequestDispatcher("/internal/batchee/layout.jsp").forward(req, resp);
    }

    private void doStart(final HttpServletRequest req, final String name, final Properties properties) {
        req.setAttribute("id", operator.start(name, properties));
        req.setAttribute("name", name);
        req.setAttribute("view", "after-start");
    }

    private void start(final HttpServletRequest req, final String name) {
        req.setAttribute("view", "start");
        req.setAttribute("name", name);
    }

    private void listStepExecutions(final HttpServletRequest req, final int executionId) {
        final List<StepExecution> steps = operator.getStepExecutions(executionId);
        req.setAttribute("view", "step-executions");
        req.setAttribute("steps", steps);
        req.setAttribute("executionId", executionId);
        req.setAttribute("name", operator.getJobExecution(executionId).getJobName());
    }

    private void listExecutions(final HttpServletRequest req, final String name, final int pageSize, final int inStart) {
        final int jobInstanceCount = operator.getJobInstanceCount(name);

        int start = inStart;
        if (start == -1) { // first page is last page
            start = Math.max(0, jobInstanceCount - pageSize);
        }

        final List<JobInstance> instances = new ArrayList<JobInstance>(operator.getJobInstances(name, start, pageSize));
        Collections.sort(instances, JobInstanceIdComparator.INSTANCE);

        final Map<JobInstance, List<JobExecution>> executions = new LinkedHashMap<JobInstance, List<JobExecution>>();
        for (final JobInstance instance : instances) {
            executions.put(instance, operator.getJobExecutions(instance));
        }

        req.setAttribute("view", "job-instances");
        req.setAttribute("name", name);
        req.setAttribute("executions", executions);

        int nextStart = start + pageSize;
        if (nextStart > jobInstanceCount) {
            nextStart = -1;
        }
        req.setAttribute("nextStart", nextStart);

        req.setAttribute("previousStart", start - pageSize);

        if (jobInstanceCount > pageSize) {
            req.setAttribute("lastStart", Math.max(0, jobInstanceCount - pageSize));
        } else {
            req.setAttribute("lastStart", -1);
        }
    }

    private void listJobs(final HttpServletRequest req) throws ServletException, IOException {
        Set<String> names = operator.getJobNames();
        if (names == null) {
            names = Collections.emptySet();
        }

        req.setAttribute("view", "jobs");
        req.setAttribute("names", names);
    }

    private static int extractInt(final HttpServletRequest req, final String name, final int defaultValue) {
        final String string = req.getParameter(name);
        if (string != null) {
            return Integer.parseInt(string);
        }
        return defaultValue;
    }

    private static Properties readProperties(final HttpServletRequest req) {
        final Map<String, String> map = new HashMap<String, String>();
        final Enumeration<String> names = req.getParameterNames();
        while (names.hasMoreElements()) {
            final String key = names.nextElement();
            map.put(key, req.getParameter(key));
        }

        final Properties properties = new Properties();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith("k_")) {
                final String name = key.substring("k_".length());
                properties.setProperty(name, map.get("v_" + name));
            }
        }
        return properties;
    }

    private static class JobInstanceIdComparator implements java.util.Comparator<JobInstance> {
        private static final JobInstanceIdComparator INSTANCE = new JobInstanceIdComparator();

        @Override
        public int compare(final JobInstance o1, final JobInstance o2) {
            return (int) (o2.getInstanceId() - o1.getInstanceId()); // reverse order since users will prefer last first
        }
    }
}
