/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.gui.servlet;

import java.util.HashMap;
import java.util.Map;

class MapBuilder<A, B> {
    private final Map<A, B> map = new HashMap<A, B>();

    public MapBuilder<A, B> set(final A a, final B b) {
        map.put(a, b);
        return this;
    }

    public MapBuilder<A, B> set(final Map<A, B> params) {
        map.putAll(params);
        return this;
    }

    public Map<A, B> build() {
        return map;
    }

    static class Simple extends MapBuilder<String, Object> {
    }
}

