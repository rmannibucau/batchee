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
package org.apache.batchee.camel;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemProcessor;
import javax.inject.Inject;

public class CamelItemProcessor implements ItemProcessor {
    @Inject
    @BatchProperty
    private String endpoint;

    @Inject
    @BatchProperty
    private String unwrap;

    @Override
    public Object processItem(final Object item) throws Exception {
        return CamelTask.camelize(endpoint, unwrap, item);
    }

    public void setEndpoint(final String endpoint) {
        this.endpoint = endpoint;
    }

    public void setUnwrap(final String unwrap) {
        this.unwrap = unwrap;
    }
}
