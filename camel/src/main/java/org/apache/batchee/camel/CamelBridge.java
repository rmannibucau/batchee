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

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;

public class CamelBridge {
    protected static final DefaultCamelContext CONTEXT = new DefaultCamelContext();
    protected static final ProducerTemplate PRODUCER_TEMPLATE = CONTEXT.createProducerTemplate();
    protected static final ConsumerTemplate CONSUMER_TEMPLATE = CONTEXT.createConsumerTemplate();

    protected static Object process(final String endpoint, final Object body) throws Exception {
        return PRODUCER_TEMPLATE.requestBody(endpoint, body);
    }

    public static Object receive(final String endpoint, final long timeout, final Class<?> expected) {
        if (timeout > 0) {
            if (expected != null) {
                return CONSUMER_TEMPLATE.receiveBody(endpoint, expected);
            }
            return CONSUMER_TEMPLATE.receiveBody(endpoint);
        }

        if (expected != null) {
            return CONSUMER_TEMPLATE.receiveBody(endpoint, timeout, expected);
        }
        return CONSUMER_TEMPLATE.receiveBody(endpoint, timeout);
    }
}

