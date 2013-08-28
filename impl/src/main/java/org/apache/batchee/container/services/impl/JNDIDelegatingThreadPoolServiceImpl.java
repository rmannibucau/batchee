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
import org.apache.batchee.spi.services.BatchThreadPoolService;
import org.apache.batchee.spi.services.IBatchConfig;
import org.apache.batchee.spi.services.ParallelTaskResult;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class JNDIDelegatingThreadPoolServiceImpl implements BatchThreadPoolService {
    public static final String THREADPOOL_JNDI_LOCATION = "THREADPOOL_JNDI_LOCATION";

    public final String DEFAULT_JNDI_LOCATION = "java:comp/DefaultManagedExecutorService";

    private String jndiLocation = null;

    @Override
    public void init(IBatchConfig batchConfig) {
        // Don't want to get/cache the actual threadpool here since we want to do a JNDI lookup each time.
        jndiLocation = batchConfig.getConfigProperties().getProperty(THREADPOOL_JNDI_LOCATION, DEFAULT_JNDI_LOCATION);
    }

    public void shutdown() throws BatchContainerServiceException {
        // no-op
    }

    public void executeTask(Runnable work, Object config) {
        try {
            final Context ctx = new InitialContext();
            final ExecutorService delegateService = (ExecutorService) ctx.lookup(jndiLocation);
            delegateService.execute(work);
        } catch (final NamingException e) {
            throw new BatchContainerServiceException(e);
        }
    }

    public ParallelTaskResult executeParallelTask(Runnable work, Object config) {
        try {
            final Context ctx = new InitialContext();
            final ExecutorService delegateService = (ExecutorService) ctx.lookup(jndiLocation);
            final Future result = delegateService.submit(work);
            return new JSEResultAdapter(result);
        } catch (final NamingException e) {
            throw new BatchContainerServiceException(e);
        }
    }
}
