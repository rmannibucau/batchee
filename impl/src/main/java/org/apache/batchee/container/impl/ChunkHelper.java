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
package org.apache.batchee.container.impl;

import org.apache.batchee.jaxb.Chunk;

public class ChunkHelper {

    public static int getItemCount(Chunk chunk) {
        String chunkSizeStr = chunk.getItemCount();
        int size = 10;

        if (chunkSizeStr != null && !chunkSizeStr.isEmpty()) {
            size = Integer.valueOf(chunk.getItemCount());
        }

        chunk.setItemCount(Integer.toString(size));
        return size;
    }

    public static int getTimeLimit(Chunk chunk) {
        String chunkTimeLimitStr = chunk.getTimeLimit();
        int timeLimit = 0; //default time limit = 0 seconds ie no timelimit

        if (chunkTimeLimitStr != null && !chunkTimeLimitStr.isEmpty()) {
            timeLimit = Integer.valueOf(chunk.getTimeLimit());
        }

        chunk.setTimeLimit(Integer.toString(timeLimit));
        return timeLimit;
    }

    public static String getCheckpointPolicy(Chunk chunk) {
        String checkpointPolicy = chunk.getCheckpointPolicy();

        if (checkpointPolicy != null && !checkpointPolicy.isEmpty()) {
            if (!(checkpointPolicy.equals("item") || checkpointPolicy.equals("custom"))) {
                throw new IllegalArgumentException("The only supported attributed values for 'checkpoint-policy' are 'item' and 'custom'.");
            }
        } else {
            checkpointPolicy = "item";
        }

        chunk.setCheckpointPolicy(checkpointPolicy);
        return checkpointPolicy;
    }

    public static int getSkipLimit(Chunk chunk) {
        return Integer.valueOf(chunk.getSkipLimit());
    }

    public static int getRetryLimit(Chunk chunk) {
        return Integer.valueOf(chunk.getRetryLimit());
    }

}
