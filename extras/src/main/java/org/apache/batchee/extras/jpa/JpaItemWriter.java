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
import javax.batch.api.chunk.ItemWriter;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import java.io.Serializable;
import java.util.List;

public class JpaItemWriter implements ItemWriter {
    @Inject
    @BatchProperty
    private String entityManagerProvider;

    @Inject
    @BatchProperty
    private String useMerge;

    private EntityManagerProvider emProvider;
    private boolean merge;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        emProvider = EntityManagerProvider.class.cast(Thread.currentThread().getContextClassLoader().loadClass(entityManagerProvider));
        merge = Boolean.parseBoolean(useMerge);
    }

    @Override
    public void close() throws Exception {
        // no-op
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        final EntityManager em = emProvider.newEntityManager();
        try {
            for (final Object o : items) {
                if (!merge) {
                    em.persist(o);
                } else {
                    em.merge(o);
                }
            }
            em.flush();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }
}
