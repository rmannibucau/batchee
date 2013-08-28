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
package org.apache.batchee.container.services.impl;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.spi.BatchSPIManager;
import org.apache.batchee.spi.services.IBatchConfig;
import org.apache.batchee.spi.services.BatchThreadPoolService;
import org.apache.batchee.spi.services.ParallelTaskResult;

import java.util.concurrent.ExecutorService;

public class SPIDelegatingThreadPoolServiceImpl implements BatchThreadPoolService {
    @Override
    public void init(final IBatchConfig batchConfig) {
        // we want to call getExecutorServiceProvider() each time
    }

    public void shutdown() throws BatchContainerServiceException {
        executorService().shutdownNow();
    }

    public void executeTask(final Runnable work, final Object config) {
        executorService().execute(work);
    }

    public ParallelTaskResult executeParallelTask(final Runnable work, final Object config) {
        return new JSEResultAdapter(executorService().submit(work));
    }

    private static ExecutorService executorService() {
        return BatchSPIManager.getInstance().getExecutorServiceProvider().getExecutorService();
    }
}
