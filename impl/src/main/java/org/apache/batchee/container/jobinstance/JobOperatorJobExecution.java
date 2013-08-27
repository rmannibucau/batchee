/**
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
package org.apache.batchee.container.jobinstance;

import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.services.IJobExecution;
import org.apache.batchee.container.services.IPersistenceManagerService;
import org.apache.batchee.container.services.IPersistenceManagerService.TimestampType;
import org.apache.batchee.container.servicesmanager.ServicesManager;
import org.apache.batchee.container.servicesmanager.ServicesManagerImpl;
import org.apache.batchee.spi.TaggedJobExecution;

import javax.batch.runtime.BatchStatus;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

public class JobOperatorJobExecution implements IJobExecution, TaggedJobExecution {
    private static ServicesManager servicesManager = ServicesManagerImpl.getInstance();
    private static IPersistenceManagerService _persistenceManagementService = servicesManager.getPersistenceManagerService();

    private long executionID = 0L;
    private long instanceID = 0L;

    private Timestamp createTime;
    private Timestamp startTime;
    private Timestamp endTime;
    private Timestamp updateTime;
    private String batchStatus;
    private String exitStatus;
    private Properties jobProperties = null;
    private String jobName = null;
    private JobContextImpl jobContext = null;

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setJobContext(JobContextImpl jobContext) {
        this.jobContext = jobContext;
    }

    public JobOperatorJobExecution(long executionId, long instanceId) {
        this.executionID = executionId;
        this.instanceID = instanceId;
    }

    @Override
    public BatchStatus getBatchStatus() {

        BatchStatus batchStatus;

        if (this.jobContext != null) {
            batchStatus = this.jobContext.getBatchStatus();
        } else {
            // old job, retrieve from the backend
            batchStatus = BatchStatus.valueOf(_persistenceManagementService.jobOperatorQueryJobExecutionBatchStatus(executionID));
        }
        return batchStatus;
    }

    @Override
    public Date getCreateTime() {

        if (this.jobContext == null) {
            createTime = _persistenceManagementService.jobOperatorQueryJobExecutionTimestamp(executionID, TimestampType.CREATE);
        }

        if (createTime != null) {
            return new Date(createTime.getTime());
        } else return createTime;
    }

    @Override
    public Date getEndTime() {


        if (this.jobContext == null) {
            endTime = _persistenceManagementService.jobOperatorQueryJobExecutionTimestamp(executionID, TimestampType.END);
        }

        if (endTime != null) {
            return new Date(endTime.getTime());
        } else return endTime;
    }

    @Override
    public long getExecutionId() {
        return executionID;
    }

    @Override
    public String getExitStatus() {

        if (this.jobContext != null) {
            return this.jobContext.getExitStatus();
        } else {
            exitStatus = _persistenceManagementService.jobOperatorQueryJobExecutionExitStatus(executionID);
            return exitStatus;
        }

    }

    @Override
    public Date getLastUpdatedTime() {

        if (this.jobContext == null) {
            this.updateTime = _persistenceManagementService.jobOperatorQueryJobExecutionTimestamp(executionID, TimestampType.LAST_UPDATED);
        }

        if (updateTime != null) {
            return new Date(this.updateTime.getTime());
        } else return updateTime;
    }

    @Override
    public Date getStartTime() {

        if (this.jobContext == null) {
            startTime = _persistenceManagementService.jobOperatorQueryJobExecutionTimestamp(executionID, TimestampType.STARTED);
        }

        if (startTime != null) {
            return new Date(startTime.getTime());
        } else return startTime;
    }

    @Override
    public Properties getJobParameters() {
        // TODO Auto-generated method stub
        return jobProperties;
    }

    // IMPL specific setters

    public void setBatchStatus(String status) {
        batchStatus = status;
    }

    public void setCreateTime(Timestamp ts) {
        createTime = ts;
    }

    public void setEndTime(Timestamp ts) {
        endTime = ts;
    }

    public void setExecutionId(long id) {
        executionID = id;
    }

    public void setJobInstanceId(long jobInstanceID) {
        instanceID = jobInstanceID;
    }

    public void setExitStatus(String status) {
        exitStatus = status;

    }

    public void setInstanceId(long id) {
        instanceID = id;
    }

    public void setLastUpdateTime(Timestamp ts) {
        updateTime = ts;
    }

    public void setStartTime(Timestamp ts) {
        startTime = ts;
    }

    public void setJobParameters(Properties jProps) {
        jobProperties = jProps;
    }

    @Override
    public String getJobName() {
        return jobName;
    }

    @Override
    public String getTagName() {
        return _persistenceManagementService.getTagName(executionID);
    }

    @Override
    public long getInstanceId() {
        return instanceID;
    }

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("createTime=" + createTime);
        buf.append(",batchStatus=" + batchStatus);
        buf.append(",exitStatus=" + exitStatus);
        buf.append(",jobName=" + jobName);
        buf.append(",instanceId=" + instanceID);
        buf.append(",executionId=" + executionID);
        return buf.toString();
    }

}
