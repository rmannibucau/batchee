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
package org.apache.batchee.extras.jdbc;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.inject.Inject;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;

public class JdbcReader extends JdbcConnectionConfiguration implements ItemReader {
    @Inject
    @BatchProperty(name = "mapper")
    private String mapperStr;

    @Inject
    @BatchProperty
    private String query;

    private LinkedList<Object> items;
    private RecordMapper mapper;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        mapper = RecordMapper.class.cast(Thread.currentThread().getContextClassLoader().loadClass(mapperStr).newInstance());
        items = new LinkedList<Object>();
    }

    @Override
    public void close() throws Exception {
        // no-op
    }

    @Override
    public Object readItem() throws Exception {
        if (items.isEmpty()) {
            final Connection conn = connection();
            final PreparedStatement preparedStatement = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet resultSet = null;
            try {
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    items.add(mapper.map(resultSet));
                }
                if (items.isEmpty()) {
                    return null;
                }
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
                preparedStatement.close();
            }
        }
        return items.pop();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null; // datasource can be JtaManaged in a container supporting it
    }
}
