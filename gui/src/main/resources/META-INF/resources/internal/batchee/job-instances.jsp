<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<%@ page import="javax.batch.runtime.JobExecution" %>
<%@ page import="javax.batch.runtime.JobInstance" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Map" %>
<%@ page import="javax.xml.bind.DatatypeConverter" %>
<%@ page import="org.apache.batchee.gui.servlet.StatusHelper" %>

<% final String name = (String) request.getAttribute("name"); %>
<h4><%= name %></h4>

<% for ( final Map.Entry<JobInstance, List<JobExecution>> instance : ((Map<JobInstance, List<JobExecution>>) request.getAttribute("executions")).entrySet() ) { %>
    <h5>Executions of instance <%= instance.getKey().getInstanceId() %></h5>

    <table class="table tabl-hover">
        <thead>
        <tr>
            <th>#</th>
            <th>Batch status</th>
            <th>Exit status</th>
            <th>Create time</th>
            <th>Last updated time</th>
            <th>End time</th>
        </tr>
        </thead>
        <tbody>
        <% for ( final JobExecution execution : instance.getValue() ) { %>
            <tr class="<%= StatusHelper.statusClass(execution.getBatchStatus()) %>">
                <td><a href="<%= request.getAttribute("mapping") %>/step-executions/<%= execution.getExecutionId() %>"><%= execution.getExecutionId() %></a></td>
                <td><%= execution.getBatchStatus().name() %></td>
                <td><%= execution.getExitStatus() %></td>
                <td><%= execution.getCreateTime() %></td>
                <td><%= execution.getLastUpdatedTime() %></td>
                <td><%= execution.getEndTime() %></td>
            </tr>
        <% } %>
        </tbody>
    </table>

<%
    final Integer nextStart = (Integer) request.getAttribute("nextStart");
    if (nextStart > 0) {
%>
    <a href="<%= request.getAttribute("mapping") %>/executions/<%= DatatypeConverter.printBase64Binary(name.getBytes()) %>?start=<%= nextStart %>">Next executions</a>
<%
    }
%>

<% } %>
