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
package org.apache.batchee.extras.transaction;

import org.apache.batchee.extras.transaction.integration.SynchronizationService;
import org.apache.batchee.extras.transaction.integration.Synchronizations;

import javax.batch.api.chunk.ItemReader;
import java.io.Serializable;

public abstract class TransactionalReader implements ItemReader {
    private static final String READER_COUNT = TransactionalReader.class.getName() + ".reader-count";

    private final String key;
    protected long items = 0;

    protected TransactionalReader() {
        key = READER_COUNT + hashCode();
    }

    protected void incrementReaderCount() {
        if (Synchronizations.hasTransaction()) {
            Long count = (Long) Synchronizations.get(key);
            if (count == null) {
                count = items;
                Synchronizations.registerSynchronization(new SynchronizationService.OnCommit() {
                    @Override
                    public void afterCommit() {
                        Integer max = (Integer) Synchronizations.get(key);
                        if (max == null) {
                            return;
                        }

                        incrementCount(max);
                    }
                });
            }
            Synchronizations.put(key, count + 1);
        } else {
            incrementCount(1);
        }
    }

    protected void incrementCount(int number) {
        items += number;
    }

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        if (checkpoint != null && Number.class.isInstance(checkpoint)) {
            items = Number.class.cast(checkpoint).longValue();
            if (items > 0) {
                int i = 0;
                Object l;
                do {
                    l = doRead();
                    i++;
                } while (l != null && i < items);
            }
        }
    }

    protected abstract Object doRead() throws Exception;

    @Override
    public Object readItem() throws Exception {
        final Object s = doRead();
        if (s != null) {
            incrementReaderCount();
        }
        return s;
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return items;
    }
}
