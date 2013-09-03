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
package org.apache.batchee.container.proxy;

import org.apache.batchee.container.impl.StepContextImpl;

/**
 * An abstract class which contains the common behavior for a batch artifact
 * proxy. This class performs runtime introspection of an artifact instances
 * annotations and handles property injection.
 */
public abstract class AbstractProxy<T> {
    protected T delegate;
    protected StepContextImpl stepContext;

    /**
     * @param delegate An instance of a batch artifact which will back this proxy
     */
    AbstractProxy(T delegate) {
        this.delegate = delegate;
    }

    public T getDelegate() {
        return this.delegate;
    }

    public void setStepContext(final StepContextImpl stepContext) {
        this.stepContext = stepContext;
    }
}
