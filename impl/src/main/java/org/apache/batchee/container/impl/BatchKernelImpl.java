/*
 * Copyright 2012 International Business Machines Corp.
 * 
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.container.impl;

import org.apache.batchee.container.IThreadRootController;
import org.apache.batchee.container.callback.IJobEndCallbackService;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.jobinstance.JobExecutionHelper;
import org.apache.batchee.container.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.IBatchKernelService;
import org.apache.batchee.container.services.IJobExecution;
import org.apache.batchee.container.services.IPersistenceManagerService;
import org.apache.batchee.container.services.impl.NoOpBatchSecurityHelper;
import org.apache.batchee.container.services.impl.RuntimeBatchJobUtil;
import org.apache.batchee.container.servicesmanager.ServicesManager;
import org.apache.batchee.container.servicesmanager.ServicesManagerImpl;
import org.apache.batchee.container.util.BatchFlowInSplitWorkUnit;
import org.apache.batchee.container.util.BatchPartitionWorkUnit;
import org.apache.batchee.container.util.BatchWorkUnit;
import org.apache.batchee.container.util.FlowInSplitBuilderConfig;
import org.apache.batchee.container.util.PartitionsBuilderConfig;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.spi.BatchJobUtil;
import org.apache.batchee.spi.BatchSPIManager;
import org.apache.batchee.spi.BatchSecurityHelper;
import org.apache.batchee.spi.services.IBatchConfig;
import org.apache.batchee.spi.services.IBatchThreadPoolService;

import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionNotMostRecentException;
import javax.batch.operations.JobExecutionNotRunningException;
import javax.batch.operations.JobRestartException;
import javax.batch.operations.JobStartException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.JobInstance;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BatchKernelImpl implements IBatchKernelService {
    public static final Properties EMPTY_PROPS = new Properties();
    private Map<Long, IThreadRootController> executionId2jobControllerMap = new ConcurrentHashMap<Long, IThreadRootController>();
    private Set<Long> instanceIdExecutingSet = new HashSet<Long>();

    ServicesManager servicesManager = ServicesManagerImpl.getInstance();

    private IBatchThreadPoolService executorService = null;

    private IJobEndCallbackService callbackService = null;

    private IPersistenceManagerService persistenceService = null;

    private BatchSecurityHelper batchSecurity = null;

    private BatchJobUtil batchJobUtil = null;

    public BatchKernelImpl() {
        executorService = servicesManager.getThreadPoolService();
        callbackService = servicesManager.getJobCallbackService();
        persistenceService = servicesManager.getPersistenceManagerService();

        // registering our implementation of the util class used to purge by apptag
        batchJobUtil = new RuntimeBatchJobUtil();
        BatchSPIManager.getInstance().registerBatchJobUtil(batchJobUtil);
    }

    public BatchSecurityHelper getBatchSecurityHelper() {
        batchSecurity = BatchSPIManager.getInstance().getBatchSecurityHelper();
        if (batchSecurity == null) {
            batchSecurity = new NoOpBatchSecurityHelper();
        }
        return batchSecurity;
    }

    @Override
    public void init(final IBatchConfig pgcConfig) throws BatchContainerServiceException {
        // no-op
    }

    @Override
    public void shutdown() throws BatchContainerServiceException {
        // no-op

    }

    @Override
    public IJobExecution startJob(final String jobXML) throws JobStartException {
        return startJob(jobXML, null);
    }

    @Override
    public IJobExecution startJob(final String jobXML, final Properties jobParameters) throws JobStartException {
        final RuntimeJobExecution jobExecution = JobExecutionHelper.startJob(jobXML, jobParameters);

        // TODO - register with status manager

        final BatchWorkUnit batchWork = new BatchWorkUnit(this, jobExecution);
        registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

        executorService.executeTask(batchWork, null);

        return jobExecution.getJobOperatorJobExecution();
    }

    @Override
    public void stopJob(final long executionId) throws NoSuchJobExecutionException, JobExecutionNotRunningException {

        final IThreadRootController controller = this.executionId2jobControllerMap.get(executionId);
        if (controller == null) {
            throw new JobExecutionNotRunningException("JobExecution with execution id of " + executionId + "is not running.");
        }
        controller.stop();
    }

    @Override
    public IJobExecution restartJob(final long executionId) throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {
        return restartJob(executionId, EMPTY_PROPS);
    }

    @Override
    public IJobExecution restartJob(final long executionId, final Properties jobOverrideProps) throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {
        final RuntimeJobExecution jobExecution = JobExecutionHelper.restartJob(executionId, jobOverrideProps);
        final BatchWorkUnit batchWork = new BatchWorkUnit(this, jobExecution);

        registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

        executorService.executeTask(batchWork, null);

        return jobExecution.getJobOperatorJobExecution();
    }

    @Override
    public void jobExecutionDone(final RuntimeJobExecution jobExecution) {
        callbackService.done(jobExecution.getExecutionId());

        // Remove from executionId, instanceId map,set after job is done
        this.executionId2jobControllerMap.remove(jobExecution.getExecutionId());
        this.instanceIdExecutingSet.remove(jobExecution.getInstanceId());

        // AJM: ah - purge jobExecution from map here and flush to DB?
        // edit: no long want a 2 tier for the jobexecution...do want it for step execution
        // renamed method to flushAndRemoveStepExecution

    }

    public IJobExecution getJobExecution(final long executionId) throws NoSuchJobExecutionException {
        return JobExecutionHelper.getPersistedJobOperatorJobExecution(executionId);
    }

    @Override
    public void startGeneratedJob(final BatchWorkUnit batchWork) {
        //This call is non-blocking
        executorService.executeParallelTask(batchWork, null);
    }

    @Override
    public int getJobInstanceCount(final String jobName) {
        return persistenceService.jobOperatorGetJobInstanceCount(jobName);
    }

    @Override
    public JobInstance getJobInstance(final long executionId) {
        return JobExecutionHelper.getJobInstance(executionId);
    }


    /**
     * Build a list of batch work units and set them up in STARTING state but don't start them yet.
     */

    @Override
    public List<BatchPartitionWorkUnit> buildNewParallelPartitions(final PartitionsBuilderConfig config)
        throws JobRestartException, JobStartException {

        final List<JSLJob> jobModels = config.getJobModels();
        final Properties[] partitionPropertiesArray = config.getPartitionProperties();
        final List<BatchPartitionWorkUnit> batchWorkUnits = new ArrayList<BatchPartitionWorkUnit>(jobModels.size());

        int instance = 0;
        for (final JSLJob parallelJob : jobModels) {
            final Properties partitionProps = (partitionPropertiesArray == null) ? null : partitionPropertiesArray[instance];
            final RuntimeJobExecution jobExecution = JobExecutionHelper.startPartition(parallelJob, partitionProps);
            jobExecution.setPartitionInstance(instance);

            final BatchPartitionWorkUnit batchWork = new BatchPartitionWorkUnit(this, jobExecution, config);

            registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

            batchWorkUnits.add(batchWork);
            instance++;
        }

        return batchWorkUnits;
    }

    @Override
    public List<BatchPartitionWorkUnit> buildOnRestartParallelPartitions(final PartitionsBuilderConfig config) throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException {

        final List<JSLJob> jobModels = config.getJobModels();
        final Properties[] partitionProperties = config.getPartitionProperties();
        final List<BatchPartitionWorkUnit> batchWorkUnits = new ArrayList<BatchPartitionWorkUnit>(jobModels.size());

        //for now let always use a Properties array. We can add some more convenience methods later for null properties and what not

        int instance = 0;
        for (final JSLJob parallelJob : jobModels) {

            final Properties partitionProps = (partitionProperties == null) ? null : partitionProperties[instance];

            try {
                final long execId = getMostRecentExecutionId(parallelJob);
                final RuntimeJobExecution jobExecution;
                try {
                    jobExecution = JobExecutionHelper.restartPartition(execId, parallelJob, partitionProps);
                    jobExecution.setPartitionInstance(instance);
                } catch (final NoSuchJobExecutionException e) {
                    throw new IllegalStateException("Caught NoSuchJobExecutionException but this is an internal JobExecution so this shouldn't have happened: execId =" + execId, e);
                }

                final BatchPartitionWorkUnit batchWork = new BatchPartitionWorkUnit(this, jobExecution, config);
                registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

                batchWorkUnits.add(batchWork);
            } catch (final JobExecutionAlreadyCompleteException e) {
                // no-op
            }

            instance++;
        }

        return batchWorkUnits;
    }

    @Override
    public void restartGeneratedJob(final BatchWorkUnit batchWork) throws JobRestartException {
        //This call is non-blocking
        executorService.executeParallelTask(batchWork, null);
    }

    @Override
    public BatchFlowInSplitWorkUnit buildNewFlowInSplitWorkUnit(final FlowInSplitBuilderConfig config) {
        final JSLJob parallelJob = config.getJobModel();

        final RuntimeFlowInSplitExecution execution = JobExecutionHelper.startFlowInSplit(parallelJob);
        final BatchFlowInSplitWorkUnit batchWork = new BatchFlowInSplitWorkUnit(this, execution, config);

        registerCurrentInstanceAndExecution(execution, batchWork.getController());
        return batchWork;
    }

    private long getMostRecentExecutionId(final JSLJob jobModel) {

        //There can only be one instance associated with a subjob's id since it is generated from an unique
        //job instance id. So there should be no way to directly start a subjob with particular
        final List<Long> instanceIds = persistenceService.jobOperatorGetJobInstanceIds(jobModel.getId(), 0, 2);

        // Maybe we should blow up on '0' too?
        if (instanceIds.size() > 1) {
            throw new IllegalStateException("Found " + instanceIds.size() + " entries for instance id = " + jobModel.getId() + ", which should not have happened.  Blowing up.");
        }

        final List<IJobExecution> partitionExecs = persistenceService.jobOperatorGetJobExecutions(instanceIds.get(0));

        Long execId = Long.MIN_VALUE;
        for (final IJobExecution partitionExec : partitionExecs) {
            if (partitionExec.getExecutionId() > execId) {
                execId = partitionExec.getExecutionId();
            }
        }
        return execId;
    }

    @Override
    public BatchFlowInSplitWorkUnit buildOnRestartFlowInSplitWorkUnit(final FlowInSplitBuilderConfig config)
        throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException {

        final JSLJob jobModel = config.getJobModel();
        final long execId = getMostRecentExecutionId(jobModel);
        final RuntimeFlowInSplitExecution jobExecution;
        try {
            jobExecution = JobExecutionHelper.restartFlowInSplit(execId, jobModel);
        } catch (final NoSuchJobExecutionException e) {
            throw new IllegalStateException("Caught NoSuchJobExecutionException but this is an internal JobExecution so this shouldn't have happened: execId =" + execId, e);
        }

        final BatchFlowInSplitWorkUnit batchWork = new BatchFlowInSplitWorkUnit(this, jobExecution, config);

        registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());
        return batchWork;
    }

    private void registerCurrentInstanceAndExecution(final RuntimeJobExecution jobExecution, final IThreadRootController controller) {
        final long execId = jobExecution.getExecutionId();
        final long instanceId = jobExecution.getInstanceId();
        final String errorPrefix = "Tried to execute with Job executionId = " + execId + " and instanceId = " + instanceId + " ";
        if (executionId2jobControllerMap.get(execId) != null) {
            throw new IllegalStateException(errorPrefix + "but executionId is already currently executing.");
        } else if (instanceIdExecutingSet.contains(instanceId)) {
            throw new IllegalStateException(errorPrefix + "but another execution with this instanceId is already currently executing.");
        } else {
            instanceIdExecutingSet.add(instanceId);
            executionId2jobControllerMap.put(jobExecution.getExecutionId(), controller);
        }
    }

    @Override
    public boolean isExecutionRunning(final long executionId) {
        return executionId2jobControllerMap.containsKey(executionId);
    }
}
