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
package org.apache.batchee.container.services.impl;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.spi.services.IBatchArtifactFactory;
import org.apache.batchee.spi.services.IBatchConfig;

import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.naming.InitialContext;

public class CDIBatchArtifactFactoryImpl implements IBatchArtifactFactory {
    @Override
    public Object load(final String batchId) {
        Object artifactInstance = null;
        try { // TODO: use BeanManagerProvider of DeltaSpike instead of this lookup
            final InitialContext initialContext = new InitialContext();
            final BeanManager bm = (BeanManager) initialContext.lookup("java:comp/BeanManager");
            final Bean bean = bm.getBeans(batchId).iterator().next();
            Class clazz = bean.getBeanClass();
            artifactInstance = bm.getReference(bean, clazz, bm.createCreationalContext(bean));
        } catch (final Exception e) {
            // no-op
        }
        return artifactInstance;
    }

    @Override
    public void init(final IBatchConfig batchConfig) throws BatchContainerServiceException {
        // no-op
    }

    @Override
    public void shutdown() throws BatchContainerServiceException {
        // no-op
    }
}
