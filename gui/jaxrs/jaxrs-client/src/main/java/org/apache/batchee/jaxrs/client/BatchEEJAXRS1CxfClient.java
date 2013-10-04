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
package org.apache.batchee.jaxrs.client;

import org.apache.batchee.jaxrs.common.JBatchResource;
import org.apache.cxf.jaxrs.client.Client;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;

import javax.batch.runtime.JobInstance;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;

class BatchEEJAXRS1CxfClient extends BatchEEJAXRSClientBase<Object> {
    private final JBatchResource client;

    public BatchEEJAXRS1CxfClient(final String baseUrl, final Class<?> jsonProvider) {
        try {
            client = JAXRSClientFactory.create(baseUrl, JBatchResource.class, Arrays.asList(jsonProvider.newInstance()));
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected Object extractEntity(final Object o, final Type genericReturnType) {
        return o;
    }

    @Override
    protected Object doInvoke(final Method jaxrsMethod, final Method method, final Object[] args) throws Throwable {
        Object[] usedArgs = args;

        final Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 1 && JobInstance.class.equals(parameterTypes[0])) {
            if (args[0] == null) {
                usedArgs = new Object[2];
            } else {
                final JobInstance ji = JobInstance.class.cast(args[0]);
                usedArgs = new Object[] { ji.getInstanceId(), ji.getJobName() };
            }
        }

        try {
            return jaxrsMethod.invoke(client, usedArgs);
        } catch (final InvocationTargetException ite) {
            throw ite.getCause();
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected void close() {
        try { // not in cxf 2.6 but in cxf 2.7
            Client.class.getDeclaredMethod("close").invoke(client);
        } catch (final Exception e) {
            // no-op
        }
    }
}
