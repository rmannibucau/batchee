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
package org.apache.batchee.container.callback;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.spi.services.IBatchConfig;

import javax.batch.operations.JobSecurityException;
import javax.batch.operations.NoSuchJobExecutionException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This isn't deprecated in the sense that it used to work... we did this function for the
 * TCK originally and I copied it over to the RI in case we still wanted to make use of it.
 * <p/>
 * Don't use without reworking and testing this class.
 *
 * @author skurz
 * @deprecated
 */
public class JobEndCallbackManagerImpl implements IJobEndCallbackService {

    private final static String sourceClass = JobEndCallbackManagerImpl.class.getName();
    private final static Logger logger = Logger.getLogger(sourceClass);

    private Set<JobEndCallback> callbacks = new HashSet<JobEndCallback>();
    private long sleepTime = 500L;

    @Override
    public synchronized void registerJobEndCallback(final JobEndCallback callback) {
        callbacks.add(callback);
    }

    @Override
    public synchronized void deregisterJobEndCallback(final JobEndCallback callback) {
        callbacks.remove(callback);
    }

    @Override
    public synchronized void done(final long jobExecutionId) {
        completedExecutions.add(jobExecutionId);
        final JobEndCallback[] arr = callbacks.toArray(new JobEndCallback[callbacks.size()]);
        for (final JobEndCallback callback : arr) {
            callback.done(jobExecutionId);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.container.services.IBatchServiceBase#init(com.ibm.jbatch.container.IBatchConfig)
     */
    @Override
    public void init(final IBatchConfig batchConfig) throws BatchContainerServiceException {
        // no-op

    }

    /* (non-Javadoc)
     * @see org.apache.batchee.container.services.IBatchServiceBase#shutdown()
     */
    @Override
    public void shutdown() throws BatchContainerServiceException {
        // no-op
    }

    private Set<Long> completedExecutions = new HashSet<Long>();

    public long getCallbackOnTermination(final long execID, final JobEndCallback callback) throws NoSuchJobExecutionException, JobSecurityException {
        // First get the lock on the callback
        synchronized (callback) {
            // If this execution is already complete, then there's no need to wait
            if (!completedExecutions.contains(execID)) {
                // While we have the lock we'll associate this callback with the execution id
                // so we can only get notified when this particular execution id completes.
                callback.setExecutionId(execID);
                try {
                    callback.wait(sleepTime);
                } catch (InterruptedException e) {
                    // Assume we should not continue and allow this to happen without complaint.
                    // Throw a new exception.
                    throw new IllegalStateException(e);
                }
                // Now either we have the result, or we've waiting long enough and are going to bail.
                if (!completedExecutions.contains(execID)) {
                    throw new IllegalStateException("Still didn't see a result for executionId: " + execID +
                        ".  Perhaps try increasing timeout.  Or, something else may have gone wrong.");
                }
            }
        }

        // Not absolutely required since we should have things coded such that a registered
        // callback for some other execution doesn't interfere with correct notification of
        // completion of this execution.   However, it might reduce noise and facilitate
        // debug to clean things up.
        deregisterJobEndCallback(callback);

        return execID;
    }

    private class JobEndCallbackImpl implements JobEndCallback {

        // The wrapper around long is chosen so that 'null' clearly signifies 'unset',
        // since '0' does not.
        private Long executionIdObj = null;

        @Override
        public void done(long jobExecutionId) {
            synchronized (this) {

                // If we have set an execution id into the callback,
                // then only wake up the sleep if we have matched the
                // execution id.
                if (executionIdObj != null) {
                    if (executionIdObj.longValue() == jobExecutionId) {
                        this.notify();
                    }
                }

                // otherwise there is nothing to do.   We will only be sleeping
                // with an already-set execution id.
            }
        }

        public long getExecutionId() {
            return executionIdObj;
        }

        @Override
        public void setExecutionId(long jobExecutionId) {
            executionIdObj = jobExecutionId;
        }

    }
}
