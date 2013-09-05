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
package org.apache.batchee.extras.transaction.integration;

public final class Synchronizations {
    public static final SynchronizationService DELEGATE;
    public static final boolean ACTIVE;
    static {
        boolean active = true;
        SynchronizationService service;
        try {
            Synchronizations.class.getClassLoader().loadClass("javax.transaction.UserTransaction");
            service = new JTASynchronizationService("java:comp/TransactionSynchronizationRegistry");
        } catch (final Throwable e) { // NoClassDefFoundError or ClassNotFoundException
            active = false;
            service = new NoopSynchronizationService();
        }
        ACTIVE = active;
        DELEGATE = service;
    }

    private Synchronizations() {
        // no-op
    }

    public static void registerSynchronization(final SynchronizationService.Synchronization s) {
        DELEGATE.registerSynchronization(s);
    }

    public static boolean hasTransaction() {
        return DELEGATE.hasTransaction();
    }

    public static void put(final Object key, final Object value) {
        DELEGATE.put(key, value);
    }

    public static Object get(Object key) {
        return DELEGATE.get(key);
    }
}
