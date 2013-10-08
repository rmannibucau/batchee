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

import org.apache.batchee.extras.transaction.integration.Synchronizations;

import javax.batch.api.BatchProperty;
import javax.batch.api.Batchlet;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class JdbcBatchlet extends JdbcConnectionConfiguration implements Batchlet {
    @Inject
    @BatchProperty
    private String sql;

    @Override
    public String process() throws Exception {
        final Connection conn = connection();
        try {
            final PreparedStatement preparedStatement = conn.prepareStatement(sql);
            try {
                return "" + preparedStatement.executeUpdate();
            } finally {
                preparedStatement.close();
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (!Synchronizations.hasTransaction()) {
                conn.commit();
            }
            conn.close();
        }
    }

    @Override
    public void stop() throws Exception {
        // no-op
    }
}
