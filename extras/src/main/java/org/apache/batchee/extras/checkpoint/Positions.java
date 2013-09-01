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
package org.apache.batchee.extras.checkpoint;

import org.apache.batchee.extras.reader.TransactionalReader;
import org.apache.batchee.extras.transaction.TransactionalWriter;
import org.apache.batchee.extras.transaction.integration.SynchronizationService;
import org.apache.batchee.extras.transaction.integration.Synchronizations;

import javax.batch.operations.BatchRuntimeException;
import java.io.IOException;
import java.io.Serializable;

public class Positions {
    private static final String READER_COUNT = Positions.class.getName() + ".reader-count";

    public static void reset(final TransactionalWriter writer, final Serializable checkpoint) throws IOException {
        final long restartOffset;
        if (checkpoint != null && Number.class.isInstance(checkpoint)) {
            restartOffset = Number.class.cast(checkpoint).longValue();
            if (writer.size() < restartOffset) {
                throw new BatchRuntimeException("File seems too small");
            }
        } else {
            restartOffset = 0;
        }
        writer.setPosition(restartOffset);
    }

    public static void incrementReaderCount(final TransactionalReader reader) {
        if (Synchronizations.hasTransaction()) {
            Integer count = (Integer) Synchronizations.get(READER_COUNT);
            if (count == null) {
                count = 0;
                Synchronizations.registerSynchronization(new SynchronizationService.Synchronization() {
                    @Override
                    public void beforeCompletion() {
                        // no-op
                    }

                    @Override
                    public void afterRollback() {
                        // no-op
                    }

                    @Override
                    public void afterCommit() {
                        Integer max = (Integer) Synchronizations.get(READER_COUNT);
                        if (max == null) {
                            return;
                        }

                        for (int i = 0; i < max; i++) {
                            reader.incrementCount();
                        }
                    }
                });
            }
            Synchronizations.put(READER_COUNT, count + 1);
        } else {
            reader.incrementCount();
        }
    }

    private Positions() {
        // no-op
    }
}
