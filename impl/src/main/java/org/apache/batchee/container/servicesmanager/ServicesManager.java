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
package org.apache.batchee.container.servicesmanager;

import org.apache.batchee.container.services.IBatchKernelService;
import org.apache.batchee.container.services.IJobStatusManagerService;
import org.apache.batchee.container.services.IPersistenceManagerService;
import org.apache.batchee.spi.services.IBatchArtifactFactory;
import org.apache.batchee.spi.services.IBatchThreadPoolService;
import org.apache.batchee.spi.services.IJobXMLLoaderService;
import org.apache.batchee.spi.services.ITransactionManagementService;

public interface ServicesManager {
    public IPersistenceManagerService getPersistenceManagerService();

    public IJobStatusManagerService getJobStatusManagerService();

    public ITransactionManagementService getTransactionManagementService();

    public IBatchKernelService getBatchKernelService();

    public IJobXMLLoaderService getDelegatingJobXMLLoaderService();

    public IJobXMLLoaderService getPreferredJobXMLLoaderService();

    public IBatchThreadPoolService getThreadPoolService();

    public IBatchArtifactFactory getDelegatingArtifactFactory();

    public IBatchArtifactFactory getPreferredArtifactFactory();
}