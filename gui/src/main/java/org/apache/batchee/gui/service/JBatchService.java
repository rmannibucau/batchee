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
package org.apache.batchee.gui.service;

import javax.annotation.PostConstruct;
import javax.batch.operations.JobOperator;
import javax.batch.operations.JobSecurityException;
import javax.batch.operations.NoSuchJobException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.operations.NoSuchJobInstanceException;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.JobInstance;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Set;

@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_JSON })
public class JBatchService {
    private JobOperator operator;

    @PostConstruct
    public void findOperator() { // avoid to ServiceLoader.load everytimes
        operator = BatchRuntime.getJobOperator();
    }

    @GET
    @Path("job-names")
    public String[] getJobNames() throws JobSecurityException {
        final Set<String> jobNames = operator.getJobNames();
        return jobNames.toArray(new String[jobNames.size()]);
    }

    @GET
    @Path("job-instance/count/{name}")
    public int getJobInstanceCount(final @PathParam("name") String jobName) throws NoSuchJobException, JobSecurityException {
        return operator.getJobInstanceCount(jobName);
    }

    @GET
    @Path("job-instances/{name}")
    public RestJobInstance[] getJobInstances(final @PathParam("name") String jobName, final @QueryParam("start") int start, final @QueryParam("count")  int count) throws NoSuchJobException, JobSecurityException {
        final List<RestJobInstance> restJobInstances = RestJobInstance.wrap(operator.getJobInstances(jobName, start, count));
        return restJobInstances.toArray(new RestJobInstance[restJobInstances.size()]);
    }

    @GET
    @Path("executions/running/{name}")
    public Long[] getRunningExecutions(final @PathParam("name") String jobName) throws NoSuchJobException, JobSecurityException {
        final List<Long> runningExecutions = operator.getRunningExecutions(jobName);
        return runningExecutions.toArray(runningExecutions.toArray(new Long[runningExecutions.size()]));
    }

    @GET
    @Path("execution/parameter/{id}")
    public RestProperties getParameters(final @PathParam("id") long executionId) throws NoSuchJobExecutionException, JobSecurityException {
        return RestProperties.wrap(operator.getParameters(executionId));
    }

    @GET
    @Path("job-instance/{id}")
    public RestJobInstance getJobInstance(final @PathParam("id") long executionId) throws NoSuchJobExecutionException, JobSecurityException {
        return RestJobInstance.wrap(operator.getJobInstance(executionId));
    }

    @GET
    @Path("job-executions/{id}/{name}")
    public RestJobExecution[] getJobExecutions(final @PathParam("id") long id, final @PathParam("name") String name) throws NoSuchJobInstanceException, JobSecurityException {
        final List<RestJobExecution> restJobExecutions = RestJobExecution.wrap(operator.getJobExecutions(new JobInstanceImpl(name, id)));
        return restJobExecutions.toArray(new RestJobExecution[restJobExecutions.size()]);
    }

    @GET
    @Path("job-execution/{id}")
    public RestJobExecution getJobExecution(final @PathParam("id") long executionId) throws NoSuchJobExecutionException, JobSecurityException {
        return RestJobExecution.wrap(operator.getJobExecution(executionId));
    }

    @GET
    @Path("step-executions/{id}")
    public RestStepExecution[] getStepExecutions(final @PathParam("id") long jobExecutionId) throws NoSuchJobExecutionException, JobSecurityException {
        final List<RestStepExecution> restStepExecutions = RestStepExecution.wrap(operator.getStepExecutions(jobExecutionId));
        return restStepExecutions.toArray(new RestStepExecution[restStepExecutions.size()]);
    }

    /* TODO: discuss if it should be exposed or not since it is no more read-only methods, would surely needs security
    @POST
    @Path("execution/start/{name}")
    public long start(final @PathParam("name") String jobXMLName, final RestProperties jobParameters) throws JobStartException, JobSecurityException {
        return operator.start(jobXMLName, RestProperties.unwrap(jobParameters));
    }

    @POST
    @Path("execution/restart/{id}")
    public long restart(final @PathParam("id") long executionId, final RestProperties restartParameters) throws JobExecutionAlreadyCompleteException, NoSuchJobExecutionException, JobExecutionNotMostRecentException, JobRestartException, JobSecurityException {
        return operator.restart(executionId, RestProperties.unwrap(restartParameters));
    }

    @HEAD
    @Path("execution/stop/{id}")
    public void stop(final @PathParam("id") long executionId) throws NoSuchJobExecutionException, JobExecutionNotRunningException, JobSecurityException {
        operator.stop(executionId);
    }

    @HEAD
    @Path("execution/abandon/{id}")
    public void abandon(final @PathParam("id") long executionId) throws NoSuchJobExecutionException, JobExecutionIsRunningException, JobSecurityException {
        operator.abandon(executionId);
    }
    */

    /* // needs to import jbatch impl and get the org.apache.batchee.spi.PersistenceManagerService + same note as start/restart methods ^^
    @HEAD
    @Path("clean-up/{id}")
    public void cleanUp(final @PathParam("id") long instanceId) {
        throw new UnsupportedOperationException();
    }
    */

    private static class JobInstanceImpl implements JobInstance {
        private final String name;
        private final long id;

        public JobInstanceImpl(final String name, final long id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public long getInstanceId() {
            return id;
        }

        @Override
        public String getJobName() {
            return name;
        }
    }
}
