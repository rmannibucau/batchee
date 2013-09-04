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

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultCamelContext;

public class CamelTask {
    protected static final DefaultCamelContext CONTEXT = new DefaultCamelContext();

    public static Object camelize(final String endpoint, final String unwrap, final Object body) throws Exception {
        final Exchange exchange = process(endpoint, body);
        return unwrapIfNeeded(unwrap, exchange);
    }

    public static Object unwrapIfNeeded(final String unwrap, final Exchange exchange) {
        if (unwrap == null || "true".equalsIgnoreCase(unwrap)) {
            return unwrap(exchange);
        }
        return exchange;
    }

    protected static Exchange process(final String endpoint, final Object body) throws Exception {
        final Endpoint camelEndpoint = CONTEXT.getEndpoint(endpoint);
        final Producer producer = camelEndpoint.createProducer();

        final Exchange exchange;
        if (Exchange.class.isInstance(body)) {
            exchange = Exchange.class.cast(body);
        } else {
            exchange = producer.createExchange();
            exchange.getIn().setBody(body);
        }

        producer.process(exchange);
        return exchange;
    }

    protected static Object unwrap(final Exchange exchange) {
        if (exchange.hasOut()) {
            return exchange.getOut().getBody();
        }
        return exchange.getIn().getBody();
    }
}

