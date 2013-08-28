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

import org.apache.batchee.container.cdi.BatchCDIInjectionExtension;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.spi.services.BatchArtifactFactory;
import org.apache.batchee.spi.services.IBatchConfig;

import javax.enterprise.context.Dependent;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.naming.NamingException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

public class CDIBatchArtifactFactoryImpl extends DefaultBatchArtifactFactoryImpl implements BatchArtifactFactory {
    @Override
    public Instance load(final String batchId) {
        try {
            final BeanManager bm = getBeanManager();
            final Set<Bean<?>> beans = bm.getBeans(batchId);
            final Bean<?> bean = bm.resolve(beans);
            if (bean == null) { // fallback to try to instantiate it from TCCL as per the spec
                return super.load(batchId);
            }
            final Class<?> clazz = bean.getBeanClass();
            final CreationalContext creationalContext = bm.createCreationalContext(bean);
            final Object artifactInstance = bm.getReference(bean, clazz, creationalContext);
            if (Dependent.class.equals(bean.getScope())) { // need to be released
                return new Instance(artifactInstance, new Closeable() {
                    @Override
                    public void close() throws IOException {
                        creationalContext.release();
                    }
                });
            }
            return new Instance(artifactInstance, null);
        } catch (final Exception e) {
            // no-op
        }
        return null;
    }

    @Override
    public void init(final IBatchConfig batchConfig) throws BatchContainerServiceException {
        // no-op
    }

    @Override
    public void shutdown() throws BatchContainerServiceException {
        // no-op
    }

    protected BeanManager getBeanManager() throws NamingException {
        return BatchCDIInjectionExtension.getInstance().getBeanManager();
    }
}
