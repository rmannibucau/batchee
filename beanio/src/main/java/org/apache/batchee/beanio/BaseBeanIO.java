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
package org.apache.batchee.beanio;

import org.beanio.StreamFactory;

import javax.batch.api.BatchProperty;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import java.io.InputStream;

public class BaseBeanIO {
    @Inject
    @BatchProperty(name = "file")
    protected String filePath;

    @Inject
    @BatchProperty(name = "streamName")
    protected String streamName;

    @Inject
    @BatchProperty(name = "configuration")
    protected String configuration;

    public StreamFactory open() throws Exception {
        if (filePath == null) {
            throw new BatchRuntimeException("input can't be null");
        }
        if (streamName == null) {
            streamName = filePath;
        }
        final StreamFactory streamFactory = StreamFactory.newInstance();
        if (!streamFactory.isMapped(streamName)) {
            final InputStream is = findStream();
            if (is == null) {
                throw new BatchRuntimeException("Can't find " + configuration);
            }
            try {
                streamFactory.load(is);
            } finally {
                is.close();
            }
        }
        return streamFactory;
    }

    protected InputStream findStream() {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(configuration);
    }
}
