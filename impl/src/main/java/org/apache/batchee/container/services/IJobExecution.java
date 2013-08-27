/**
 * Copyright 2013 International Business Machines Corp.
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
package org.apache.batchee.container.services;

import org.apache.batchee.container.impl.JobContextImpl;

import javax.batch.runtime.JobExecution;
import java.sql.Timestamp;
import java.util.Properties;

public interface IJobExecution extends JobExecution {

    public void setBatchStatus(String status);

    public void setCreateTime(Timestamp ts);

    public void setEndTime(Timestamp ts);

    public void setExitStatus(String status);

    public void setLastUpdateTime(Timestamp ts);

    public void setStartTime(Timestamp ts);

    public void setJobParameters(Properties jProps);

    public long getInstanceId();

    public void setJobContext(JobContextImpl jobContext);
}
