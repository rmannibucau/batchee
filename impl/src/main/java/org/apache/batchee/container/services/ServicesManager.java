/*
 * 
 * Copyright 2012,2013 International Business Machines Corp.
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
package org.apache.batchee.container.services;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.services.executor.DefaultThreadPoolService;
import org.apache.batchee.container.services.factory.DefaultBatchArtifactFactory;
import org.apache.batchee.container.services.kernel.DefaultBatchKernel;
import org.apache.batchee.container.services.loader.DefaultJobXMLLoaderService;
import org.apache.batchee.container.services.persistence.JDBCPersistenceManager;
import org.apache.batchee.container.services.security.DefaultSecurityService;
import org.apache.batchee.container.services.status.DefaultJobStatusManager;
import org.apache.batchee.container.services.transaction.DefaultBatchTransactionService;
import org.apache.batchee.container.util.BatchContainerConstants;
import org.apache.batchee.spi.BatchArtifactFactory;
import org.apache.batchee.spi.BatchService;
import org.apache.batchee.spi.BatchThreadPoolService;
import org.apache.batchee.spi.JobXMLLoaderService;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.spi.SecurityService;
import org.apache.batchee.spi.TransactionManagementService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class ServicesManager implements BatchContainerConstants {
    private final static Logger LOGGER = Logger.getLogger(ServicesManager.class.getName());

    private static final String SERVICES_CONFIGURATION_FILE = "/batchee.properties";

    // Use class names instead of Class objects to not drag in any dependencies and to easily interact with properties
    private static final Map<String, String> SERVICE_IMPL_CLASS_NAMES = new ConcurrentHashMap<String, String>();
    static {
        SERVICE_IMPL_CLASS_NAMES.put(TransactionManagementService.class.getName(), DefaultBatchTransactionService.class.getName());
        SERVICE_IMPL_CLASS_NAMES.put(PersistenceManagerService.class.getName(), JDBCPersistenceManager.class.getName());
        SERVICE_IMPL_CLASS_NAMES.put(JobStatusManagerService.class.getName(), DefaultJobStatusManager.class.getName());
        SERVICE_IMPL_CLASS_NAMES.put(BatchThreadPoolService.class.getName(), DefaultThreadPoolService.class.getName());
        SERVICE_IMPL_CLASS_NAMES.put(BatchKernelService.class.getName(), DefaultBatchKernel.class.getName());
        SERVICE_IMPL_CLASS_NAMES.put(JobXMLLoaderService.class.getName(), DefaultJobXMLLoaderService.class.getName());
        SERVICE_IMPL_CLASS_NAMES.put(BatchArtifactFactory.class.getName(), DefaultBatchArtifactFactory.class.getName());
        SERVICE_IMPL_CLASS_NAMES.put(SecurityService.class.getName(), DefaultSecurityService.class.getName());
    }

    private ServicesManager() {
        // no-op
    }

    // Lazily-loaded singleton.
    private static class Holder {
        private static final ServicesManager INSTANCE = new ServicesManager();
    }

    // Declared 'volatile' to allow use in double-checked locking.  This 'isInited'
    // refers to whether the configuration has been hardened and possibly the
    // first service impl loaded, not whether the instance has merely been instantiated.
    private final byte[] isInitedLock = new byte[0];
    private volatile Boolean isInited = Boolean.FALSE;

    private Properties batchRuntimeConfig;

    // Registry of all current services
    private final ConcurrentHashMap<String, BatchService> serviceRegistry = new ConcurrentHashMap<String, BatchService>();

    public static TransactionManagementService getTransactionManagementService() {
        return getService(TransactionManagementService.class);
    }

    public static SecurityService getSecurityService() {
        return getService(SecurityService.class);
    }

    public static BatchArtifactFactory getArtifactFactory() {
        return getService(BatchArtifactFactory.class);
    }

    public static PersistenceManagerService getPersistenceManagerService() {
        return getService(PersistenceManagerService.class);
    }

    public static JobStatusManagerService getJobStatusManagerService() {
        return getService(JobStatusManagerService.class);
    }

    public static BatchThreadPoolService getThreadPoolService() {
        return getService(BatchThreadPoolService.class);
    }

    public static BatchKernelService getBatchKernelService() {
        return getService(BatchKernelService.class);
    }

    public static JobXMLLoaderService getJobXMLLoaderService() {
        return getService(JobXMLLoaderService.class);
    }

    /**
     * Init doesn't actually load the service impls, which are still loaded lazily.   What it does is it
     * hardens the config.  This is necessary since the batch runtime by and large is not dynamically
     * configurable, (e.g. via MBeans).  Things like the database config used by the batch runtime's
     * persistent store are hardened then, as are the names of the service impls to use.
     */
    private void initIfNecessary() {
        // Use double-checked locking with volatile.
        if (!isInited) {
            synchronized (isInitedLock) {
                if (!isInited) {
                    batchRuntimeConfig = new Properties();

                    batchRuntimeConfig.putAll(SERVICE_IMPL_CLASS_NAMES);
                    batchRuntimeConfig.putAll(System.getProperties());

                    final InputStream batchServicesListInputStream = getClass().getResourceAsStream(SERVICES_CONFIGURATION_FILE);
                    if (batchServicesListInputStream != null) {
                        try {
                            batchRuntimeConfig.load(batchServicesListInputStream);
                        } catch (final Exception e) {
                            LOGGER.config("Error loading " + SERVICES_CONFIGURATION_FILE + " Exception=" + e.toString());
                        } finally {
                            try {
                                batchServicesListInputStream.close();
                            } catch (final IOException e) {
                                // no-op
                            }
                        }
                    }

                    isInited = Boolean.TRUE;
                }
            }
        }
    }

    private static <T extends BatchService> T getService(final Class<T> clazz) throws BatchContainerServiceException {
        Holder.INSTANCE.initIfNecessary();
        T service = clazz.cast(Holder.INSTANCE.serviceRegistry.get(clazz.getName()));
        if (service == null) {
            // Probably don't want to be loading two on two different threads so lock the whole table.
            synchronized (Holder.INSTANCE.serviceRegistry) {
                service = clazz.cast(Holder.INSTANCE.serviceRegistry.get(clazz.getName()));
                if (service == null) {
                    service = loadServiceHelper(clazz);
                    service.init(Holder.INSTANCE.batchRuntimeConfig);
                    Holder.INSTANCE.serviceRegistry.putIfAbsent(clazz.getName(), service);
                }
            }
        }
        return service;
    }

    private static <T extends BatchService> T loadServiceHelper(final Class<T> serviceType) {
        T service = null;
        String className = Holder.INSTANCE.batchRuntimeConfig.getProperty(serviceType.getSimpleName());
        try {
            if (className != null) {
                service = load(serviceType, className);
            } else {
                className = Holder.INSTANCE.batchRuntimeConfig.getProperty(serviceType.getName());
                if (className != null) {
                    service = load(serviceType, className);
                }
            }
        } catch (final Throwable e1) {
            throw new IllegalArgumentException("Could not instantiate service " + className + " due to exception: " + e1);
        }

        if (service == null) {
            throw new BatchContainerRuntimeException("Instantiate of service=: " + className + " returned null. Aborting...");
        }

        return service;
    }

    private static <T> T load(final Class<T> expected, final String className) throws Exception {
        final Class<?> cls = Class.forName(className);
        if (cls != null) {
            if (cls.getConstructor() != null) {
                return expected.cast(cls.newInstance());
            }
            throw new BatchContainerRuntimeException("Service class " + className + " should  have a default constructor defined");
        }
        throw new Exception("Exception loading Service class " + className + " make sure it exists");
    }
}

