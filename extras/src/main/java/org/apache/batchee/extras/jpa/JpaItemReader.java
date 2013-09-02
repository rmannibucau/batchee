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
package org.apache.batchee.extras.jpa;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

public class JpaItemReader implements ItemReader {
    @Inject
    @BatchProperty
    private String entityManagerProvider;

    @Inject
    @BatchProperty
    private String parameterProvider;

    @Inject
    @BatchProperty
    private String namedQuery;

    @Inject
    @BatchProperty
    private String query;

    @Inject
    @BatchProperty
    private int pageSize;

    private int firstResult = 0;
    private EntityManagerProvider emProvider;
    private ParameterProvider paramProvider = null;
    private LinkedList<Object> items = new LinkedList<Object>();

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        emProvider = EntityManagerProvider.class.cast(Thread.currentThread().getContextClassLoader().loadClass(entityManagerProvider));
        if (parameterProvider != null) {
            paramProvider = ParameterProvider.class.cast(Thread.currentThread().getContextClassLoader().loadClass(parameterProvider));
        }
        if (pageSize <= 0) {
            pageSize = 10;
        }
        if (namedQuery == null && query == null) {
            throw new BatchRuntimeException("a query should be provided");
        }
    }

    @Override
    public void close() throws Exception {
        // no-op
    }

    @Override
    public Object readItem() throws Exception {
        if (items.isEmpty()) {
            final Collection<?> objects = nextPage();
            if (objects == null || objects.isEmpty()) {
                return null;
            }

            items.addAll(objects);
            firstResult += items.size();
        }
        return items.pop();
    }

    private Collection<?> nextPage() {
        final EntityManager em = emProvider.newEntityManager();
        final Query jpaQuery;
        try {
            if (namedQuery != null) {
                 jpaQuery = em.createNamedQuery(namedQuery);
            } else {
                jpaQuery = em.createQuery(query);
            }
            jpaQuery.setFirstResult(firstResult).setMaxResults(pageSize);
            if (paramProvider != null) {
                paramProvider.setParameters(jpaQuery);
            }
            return jpaQuery.getResultList();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }
}
