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
package org.apache.batchee.container.servicesmanager;

import org.apache.batchee.container.callback.IJobEndCallbackService;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.exception.PersistenceException;
import org.apache.batchee.container.impl.BatchConfigImpl;
import org.apache.batchee.container.services.IBatchKernelService;
import org.apache.batchee.container.services.IJobStatusManagerService;
import org.apache.batchee.container.services.IPersistenceManagerService;
import org.apache.batchee.container.servicesmanager.ServiceTypes.Name;
import org.apache.batchee.container.util.BatchContainerConstants;
import org.apache.batchee.spi.BatchSPIManager;
import org.apache.batchee.spi.DatabaseConfigurationBean;
import org.apache.batchee.spi.services.IBatchArtifactFactory;
import org.apache.batchee.spi.services.IBatchServiceBase;
import org.apache.batchee.spi.services.IBatchThreadPoolService;
import org.apache.batchee.spi.services.IJobXMLLoaderService;
import org.apache.batchee.spi.services.ITransactionManagementService;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO: this SPI is maybe not the best one for a real world implementation
public class ServicesManagerImpl implements BatchContainerConstants, ServicesManager {
    private final static Logger logger = Logger.getLogger(ServicesManagerImpl.class.getName());

    private ServicesManagerImpl() {
        // no-op
    }

    // Lazily-loaded singleton.
    private static class ServicesManagerImplHolder {
        private static final ServicesManagerImpl INSTANCE = new ServicesManagerImpl();
    }

    public static ServicesManager getInstance() {
        return ServicesManagerImplHolder.INSTANCE;
    }

    // Declared 'volatile' to allow use in double-checked locking.  This 'isInited'
    // refers to whether the configuration has been hardened and possibly the
    // first service impl loaded, not whether the instance has merely been instantiated.
    private final byte[] isInitedLock = new byte[0];
    private volatile Boolean isInited = Boolean.FALSE;

    private DatabaseConfigurationBean databaseConfigBean = null;
    private BatchConfigImpl batchRuntimeConfig;
    private Properties batchContainerProps = null;


    private Map<Name, String> serviceImplClassNames = ServiceTypes.getServiceImplClassNames();
    private Map<String, Name> propertyNameTable = ServiceTypes.getServicePropertyNames();

    // Registry of all current services
    private final ConcurrentHashMap<Name, IBatchServiceBase> serviceRegistry = new ConcurrentHashMap<Name, IBatchServiceBase>();

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
                    batchRuntimeConfig = new BatchConfigImpl();

                    initFromPropertiesFiles();
                    initServiceImplOverrides();
                    initDatabaseConfig();
                    initOtherConfig();
                    isInited = Boolean.TRUE;
                }
            }
        }
    }

    private void initFromPropertiesFiles() {

        final Properties serviceIntegratorProps = new Properties();
        final InputStream batchServicesListInputStream = getClass().getResourceAsStream("/META-INF/services/" + BATCH_INTEGRATOR_CONFIG_FILE);

        if (batchServicesListInputStream != null) {
            try {
                logger.config("Batch Integrator Config File exists! loading it..");
                serviceIntegratorProps.load(batchServicesListInputStream);
                batchServicesListInputStream.close();
            } catch (IOException e) {
                logger.config("Error loading " + "/META-INF/services/" + BATCH_INTEGRATOR_CONFIG_FILE + " IOException=" + e.toString());
            } catch (Exception e) {
                logger.config("Error loading " + "/META-INF/services/" + BATCH_INTEGRATOR_CONFIG_FILE + " Exception=" + e.toString());
            }
        } else {
            logger.config("Could not find batch integrator config file: " + "/META-INF/services/" + BATCH_INTEGRATOR_CONFIG_FILE);
        }

        // See if any do not map to service impls.

        {
            Set<String> removeThese = new HashSet<String>();
            for (final Object key : serviceIntegratorProps.keySet()) {
                String keyStr = (String) key;
                if (!propertyNameTable.containsKey(keyStr)) {
                    logger.warning("Found property named: " + keyStr
                        + " with value: " + serviceIntegratorProps.get(keyStr)
                        + " in " + BATCH_INTEGRATOR_CONFIG_FILE + " , but did not find a corresponding service type "
                        + "in the internal table of service types.\n Ignoring this property then.   Maybe this should have been set in batch-config.properties instead.");
                    removeThese.add(keyStr);
                }
            }
            for (final String s : removeThese) {
                serviceIntegratorProps.remove(s);
            }
        }

        final Properties adminProps = new Properties();
        final InputStream batchAdminConfigListInputStream = this.getClass().getResourceAsStream("/META-INF/services/" + BATCH_ADMIN_CONFIG_FILE);

        if (batchServicesListInputStream != null) {
            try {
                logger.config("Batch Admin Config File exists! loading it..");
                adminProps.load(batchAdminConfigListInputStream);
                batchAdminConfigListInputStream.close();
            } catch (final IOException e) {
                logger.config("Error loading " + "/META-INF/services/" + BATCH_ADMIN_CONFIG_FILE + " IOException=" + e.toString());
            } catch (final Exception e) {
                logger.config("Error loading " + "/META-INF/services/" + BATCH_ADMIN_CONFIG_FILE + " Exception=" + e.toString());
            }
        } else {
            logger.config("Could not find batch admin config file: " + "/META-INF/services/" + BATCH_ADMIN_CONFIG_FILE);
        }

        // See if any DO map to service impls, which would be a mistake
        {
            Set<String> removeTheseToo = new HashSet<String>();
            for (final Object key : adminProps.keySet()) {
                String keyStr = (String) key;
                if (propertyNameTable.containsKey(keyStr)) {
                    logger.warning("Found property named: " + keyStr + " with value: " + adminProps.get(keyStr) + " in "
                        + BATCH_ADMIN_CONFIG_FILE + " , but this is a batch runtime service configuration.\n"
                        + "Ignoring this property then, since this should have been set in batch-services.properties instead.");
                    removeTheseToo.add(keyStr);
                }
            }
            for (final String s : removeTheseToo) {
                adminProps.remove(s);
            }
        }

        // Merge the two into 'batchContainerProps'
        batchContainerProps = new Properties();
        batchContainerProps.putAll(adminProps);
        batchContainerProps.putAll(serviceIntegratorProps);

        // Set this on the config.
        //
        // WARNING:  This sets us up for collisions since this is just a single holder of properties
        // potentially used by any service impl.
        batchRuntimeConfig.setConfigProperties(batchContainerProps);
    }

    private void initServiceImplOverrides() {

        // For each property we care about (i.e that defines one of our service impls)
        for (String propKey : propertyNameTable.keySet()) {
            // If the property is defined
            String value = batchContainerProps.getProperty(propKey);
            if (value != null) {
                // Get the corresponding serviceType enum and store the value of
                // the key/value property pair in the table where we store the service impl classnames.
                Name serviceType = propertyNameTable.get(propKey);
                String defaultServiceImplClassName = serviceImplClassNames.get(serviceType); // For logging.
                serviceImplClassNames.put(serviceType, value.trim());
                logger.config("Overriding serviceType: " + serviceType + ", replacing default impl classname: " +
                    defaultServiceImplClassName + " with override impl class name: " + value.trim());
            }
        }
    }

    private void initDatabaseConfig() {
        if (databaseConfigBean == null) {
            logger.config("First try to load 'suggested config' from BatchSPIManager");
            databaseConfigBean = BatchSPIManager.getInstance().getFinalDatabaseConfiguration();
            if (databaseConfigBean == null) {
                // Initialize database-related properties
                databaseConfigBean = new DatabaseConfigurationBean();
                databaseConfigBean.setJndiName(batchContainerProps.getProperty(JNDI_NAME, DEFAULT_JDBC_JNDI_NAME));
                databaseConfigBean.setJdbcDriver(batchContainerProps.getProperty(JDBC_DRIVER, DEFAULT_JDBC_DRIVER));
                databaseConfigBean.setJdbcUrl(batchContainerProps.getProperty(JDBC_URL, DEFAULT_JDBC_URL));
                databaseConfigBean.setDbUser(batchContainerProps.getProperty(DB_USER));
                databaseConfigBean.setDbPassword(batchContainerProps.getProperty(DB_PASSWORD));
                databaseConfigBean.setSchema(batchContainerProps.getProperty(DB_SCHEMA, DEFAULT_DB_SCHEMA));
            }
        } else {
            // Currently we do not expected this path to be used by Glassfish
            logger.config("Database config has been set directly from SPI, do NOT load from properties file.");
        }
        // In either case, set this bean on the main config bean
        batchRuntimeConfig.setDatabaseConfigurationBean(databaseConfigBean);
    }


    private void initOtherConfig() {
        String seMode = serviceImplClassNames.get(Name.JAVA_EDITION_IS_SE_DUMMY_SERVICE);
        if (seMode.equalsIgnoreCase("true")) {
            batchRuntimeConfig.setJ2seMode(true);
        }
    }

    // Look up registry and return requested service if exist
    // If not exist, create a new one, add to registry and return that one
    /* (non-Javadoc)
	 * @see com.ibm.jbatch.container.config.ServicesManager#getService(com.ibm.jbatch.container.config.ServicesManagerImpl.ServiceType)
	 */
    private IBatchServiceBase getService(final Name serviceType) throws BatchContainerServiceException {
        initIfNecessary();
        return new ServiceLoader(serviceType).getService();
    }

	/*
	 * 	public enum Name {
		JAVA_EDITION_IS_SE_DUMMY_SERVICE, 
		TRANSACTION_SERVICE, 
		PERSISTENCE_MANAGEMENT_SERVICE, 
		JOB_STATUS_MANAGEMENT_SERVICE, 
		BATCH_THREADPOOL_SERVICE, 
		BATCH_KERNEL_SERVICE, 
		JOB_ID_MANAGEMENT_SERVICE, 
		CALLBACK_SERVICE, 
		JOBXML_LOADER_SERVICE,                // Preferred
		DELEGATING_JOBXML_LOADER_SERVICE,      // Delegating wrapper
		CONTAINER_ARTIFACT_FACTORY_SERVICE,   // Preferred
		DELEGATING_ARTIFACT_FACTORY_SERVICE  // Delegating wrapper
	 */

    @Override
    public ITransactionManagementService getTransactionManagementService() {
        return (ITransactionManagementService) getService(Name.TRANSACTION_SERVICE);
    }

    @Override
    public IPersistenceManagerService getPersistenceManagerService() {
        return (IPersistenceManagerService) getService(Name.PERSISTENCE_MANAGEMENT_SERVICE);
    }

    @Override
    public IJobStatusManagerService getJobStatusManagerService() {
        return (IJobStatusManagerService) getService(Name.JOB_STATUS_MANAGEMENT_SERVICE);
    }

    @Override
    public IBatchThreadPoolService getThreadPoolService() {
        return (IBatchThreadPoolService) getService(Name.BATCH_THREADPOOL_SERVICE);
    }

    @Override
    public IBatchKernelService getBatchKernelService() {
        return (IBatchKernelService) getService(Name.BATCH_KERNEL_SERVICE);
    }

    @Override
    public IJobEndCallbackService getJobCallbackService() {
        return (IJobEndCallbackService) getService(Name.CALLBACK_SERVICE);
    }

    @Override
    public IJobXMLLoaderService getPreferredJobXMLLoaderService() {
        return (IJobXMLLoaderService) getService(Name.JOBXML_LOADER_SERVICE);
    }

    @Override
    public IJobXMLLoaderService getDelegatingJobXMLLoaderService() {
        return (IJobXMLLoaderService) getService(Name.DELEGATING_JOBXML_LOADER_SERVICE);
    }

    @Override
    public IBatchArtifactFactory getPreferredArtifactFactory() {
        return (IBatchArtifactFactory) getService(Name.CONTAINER_ARTIFACT_FACTORY_SERVICE);
    }

    @Override
    public IBatchArtifactFactory getDelegatingArtifactFactory() {
        return (IBatchArtifactFactory) getService(Name.DELEGATING_ARTIFACT_FACTORY_SERVICE);
    }


    private class ServiceLoader {


        private volatile IBatchServiceBase service = null;
        private Name serviceType = null;

        private ServiceLoader(Name name) {
            this.serviceType = name;
        }

        private IBatchServiceBase getService() {
            service = serviceRegistry.get(serviceType);
            if (service == null) {
                // Probably don't want to be loading two on two different threads so lock the whole table.
                synchronized (serviceRegistry) {
                    if (service == null) {
                        service = _loadServiceHelper(serviceType);
                        service.init(batchRuntimeConfig);
                        serviceRegistry.putIfAbsent(serviceType, service);
                    }
                }
            }
            return service;
        }

        /**
         * Try to load the IGridContainerService given by the className. If it fails
         * to load, default to the defaultClass. If the default fails to load, then
         * blow out of here with a RuntimeException.
         */
        private IBatchServiceBase _loadServiceHelper(Name serviceType) {
            IBatchServiceBase service = null;
            Throwable e;

            String className = serviceImplClassNames.get(serviceType);
            try {
                if (className != null)
                    service = _loadService(className);
            } catch (PersistenceException pe) {
                // Don't rewrap to make it a bit clearer
                logger.log(Level.SEVERE, "Caught persistence exception which probably means there is an issue initalizing and/or connecting to the RI database");
                throw pe;
            } catch (Throwable e1) {
                e = e1;
                logger.log(Level.SEVERE, "Could not instantiate service: " + className + " due to exception:" + e);
                throw new RuntimeException("Could not instantiate service " + className + " due to exception: " + e);
            }

            if (service == null) {
                throw new RuntimeException("Instantiate of service=: " + className + " returned null. Aborting...");
            }

            return service;
        }

        private IBatchServiceBase _loadService(final String className) throws Exception {
            final Class<?> cls = Class.forName(className);
            if (cls != null) {
                if (cls.getConstructor() != null) {
                    return IBatchServiceBase.class.cast(cls.newInstance());
                } else {
                    throw new Exception("Service class " + className + " should  have a default constructor defined");
                }
            } else {
                throw new Exception("Exception loading Service class " + className + " make sure it exists");
            }
        }
    }
}

