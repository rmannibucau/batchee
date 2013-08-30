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

import javax.batch.operations.BatchRuntimeException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;

public class ChannelPositions {
    public static void reset(final FileChannel channel, final Serializable checkpoint) throws IOException {
        final long restartOffset;
        if (checkpoint != null && Number.class.isInstance(checkpoint)) {
            restartOffset = Number.class.cast(checkpoint).longValue();
            if (channel.size() < restartOffset) {
                throw new BatchRuntimeException("File seems too small");
            }
        } else {
            restartOffset = 0;
        }
        channel.truncate(restartOffset);
        channel.position(restartOffset);
    }

    private ChannelPositions() {
        // no-op
    }
}
