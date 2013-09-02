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
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;

public abstract class JdbcConnectionConfiguration {
    @Inject
    @BatchProperty
    private String jndi;

    @Inject
    @BatchProperty
    protected String driver;

    @Inject
    @BatchProperty
    protected String url;

    @Inject
    @BatchProperty
    protected String user;

    @Inject
    @BatchProperty
    protected String password;

    protected Connection connection() throws Exception {
        if (jndi != null) {
            return DataSource.class.cast(new InitialContext().lookup(jndi)).getConnection();
        }

        try {
            Class.forName(driver);
        } catch (final ClassNotFoundException e) {
            throw new BatchRuntimeException(e);
        }
        return DriverManager.getConnection(url, user, password);
    }
}
