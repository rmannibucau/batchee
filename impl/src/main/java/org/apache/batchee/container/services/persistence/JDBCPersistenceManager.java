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
package org.apache.batchee.container.services.persistence;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.exception.PersistenceException;
import org.apache.batchee.container.impl.JobExecutionImpl;
import org.apache.batchee.container.impl.JobInstanceImpl;
import org.apache.batchee.container.impl.MetricImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.StepExecutionImpl;
import org.apache.batchee.container.impl.controller.PartitionedStepBuilder;
import org.apache.batchee.container.impl.controller.chunk.CheckpointData;
import org.apache.batchee.container.impl.controller.chunk.CheckpointDataKey;
import org.apache.batchee.container.impl.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.container.status.StepStatus;
import org.apache.batchee.container.util.TCCLObjectInputStream;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JDBCPersistenceManager implements PersistenceManagerService {
    static interface SQLConstants {
        final String JOBSTATUS_TABLE = "JOBSTATUS";
        final String STEPSTATUS_TABLE = "STEPSTATUS";
        final String CHECKPOINTDATA_TABLE = "CHECKPOINTDATA";
        final String JOBINSTANCEDATA_TABLE = "JOBINSTANCEDATA";
        final String EXECUTIONINSTANCEDATA_TABLE = "EXECUTIONINSTANCEDATA";
        final String STEPEXECUTIONINSTANCEDATA_TABLE = "STEPEXECUTIONINSTANCEDATA";

        final String CREATE_TAB_JOBSTATUS = "CREATE TABLE JOBSTATUS("
            + "id BIGINT CONSTRAINT JOBSTATUS_PK PRIMARY KEY,"
            + "obj BLOB,"
            + "CONSTRAINT JOBSTATUS_JOBINST_FK FOREIGN KEY (id) REFERENCES JOBINSTANCEDATA (jobinstanceid) ON DELETE CASCADE)";
        final String CREATE_TAB_STEPSTATUS = "CREATE TABLE STEPSTATUS("
            + "id BIGINT CONSTRAINT STEPSTATUS_PK PRIMARY KEY,"
            + "obj BLOB,"
            + "CONSTRAINT STEPSTATUS_STEPEXEC_FK FOREIGN KEY (id) REFERENCES STEPEXECUTIONINSTANCEDATA (stepexecid) ON DELETE CASCADE)";
        final String CREATE_TAB_CHECKPOINTDATA = "CREATE TABLE CHECKPOINTDATA("
            + "id VARCHAR(512),obj BLOB)";
        final String CREATE_TAB_JOBINSTANCEDATA = "CREATE TABLE JOBINSTANCEDATA("
            + "jobinstanceid BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) CONSTRAINT JOBINSTANCE_PK PRIMARY KEY,"
            + "name VARCHAR(512),"
            + "apptag VARCHAR(512))";
        final String CREATE_TAB_EXECUTIONINSTANCEDATA = "CREATE TABLE EXECUTIONINSTANCEDATA("
            + "jobexecid BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) CONSTRAINT JOBEXECUTION_PK PRIMARY KEY,"
            + "jobinstanceid BIGINT,"
            + "createtime TIMESTAMP,"
            + "starttime TIMESTAMP,"
            + "endtime TIMESTAMP,"
            + "updatetime TIMESTAMP,"
            + "parameters BLOB,"
            + "batchstatus VARCHAR(512),"
            + "exitstatus VARCHAR(512),"
            + "CONSTRAINT JOBINST_JOBEXEC_FK FOREIGN KEY (jobinstanceid) REFERENCES JOBINSTANCEDATA (jobinstanceid))";
        final String CREATE_TAB_STEPEXECUTIONINSTANCEDATA = "CREATE TABLE STEPEXECUTIONINSTANCEDATA("
            + "stepexecid BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) CONSTRAINT STEPEXECUTION_PK PRIMARY KEY,"
            + "jobexecid BIGINT,"
            + "batchstatus VARCHAR(512),"
            + "exitstatus VARCHAR(512),"
            + "stepname VARCHAR(512),"
            + "readcount INTEGER,"
            + "writecount INTEGER,"
            + "commitcount INTEGER,"
            + "rollbackcount INTEGER,"
            + "readskipcount INTEGER,"
            + "processskipcount INTEGER,"
            + "filtercount INTEGER,"
            + "writeskipcount INTEGER,"
            + "startTime TIMESTAMP,"
            + "endTime TIMESTAMP,"
            + "persistentData BLOB,"
            + "CONSTRAINT JOBEXEC_STEPEXEC_FK FOREIGN KEY (jobexecid) REFERENCES EXECUTIONINSTANCEDATA (jobexecid))";

        final String INSERT_CHECKPOINTDATA = "insert into checkpointdata values(?, ?)";

        final String UPDATE_CHECKPOINTDATA = "update checkpointdata set obj = ? where id = ?";

        final String SELECT_CHECKPOINTDATA = "select id, obj from checkpointdata where id = ?";

        final String CREATE_CHECKPOINTDATA_INDEX = "create index chk_index on checkpointdata(id)";

        // JOB OPERATOR QUERIES
        final String SELECT_JOBINSTANCEDATA_COUNT = "select count(jobinstanceid) as jobinstancecount from jobinstancedata where name = ?";

        final String SELECT_JOBINSTANCEDATA_IDS = "select jobinstanceid from jobinstancedata where name = ? order by jobinstanceid desc";
    }

    static interface Defaults {
        final String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
        final String JDBC_URL = "jdbc:derby:memory:jbatch;create=true";
        final String JDBC_USER = "app";
        final String JDBC_PASSWORD = "app";
        final String SCHEMA = "JBATCH";
    }

    protected DataSource dataSource = null;
    protected String jndiName = null;

    protected String driver = "";
    protected String schema = "";
    protected String url = "";
    protected String user = "";
    protected String pwd = "";

    @Override
    public void init(final Properties batchConfig) throws BatchContainerServiceException {
        schema = batchConfig.getProperty("persistence.database.schema", Defaults.SCHEMA);

        if (batchConfig.containsKey("persistence.database.jndi")) {
            jndiName = batchConfig.getProperty("persistence.database.jndi", "");
            if (jndiName.isEmpty()) {
                throw new BatchContainerServiceException("JNDI name is not defined.");
            }

            try {
                final Context ctx = new InitialContext();
                dataSource = DataSource.class.cast(ctx.lookup(jndiName));
            } catch (final NamingException e) {
                throw new BatchContainerServiceException(e);
            }

        } else {
            driver = batchConfig.getProperty("persistence.database.driver", Defaults.JDBC_DRIVER);
            url = batchConfig.getProperty("persistence.database.url", Defaults.JDBC_URL);
            user = batchConfig.getProperty("persistence.database.user", Defaults.JDBC_USER);
            pwd = batchConfig.getProperty("persistence.database.password", Defaults.JDBC_PASSWORD);
        }

        try {
            // only auto-create on Derby
            if ("create".equalsIgnoreCase(batchConfig.getProperty("persistence.database.ddl", "create")) && isDerby()) {
                if (!isSchemaValid()) {
                    createSchema();
                }
                checkAllTables();
            }
        } catch (final SQLException e) {
            throw new BatchContainerServiceException(e);
        }
    }

    /**
     * Checks if the default schema JBATCH or the schema defined in batch-config exists.
     *
     * @return true if the schema exists, false otherwise.
     * @throws SQLException
     */
    private boolean isSchemaValid() throws SQLException {
        final Connection conn = getConnectionToDefaultSchema();
        final DatabaseMetaData dbmd = conn.getMetaData();
        final ResultSet rs = dbmd.getSchemas();
        while (rs.next()) {
            if (schema.equalsIgnoreCase(rs.getString("TABLE_SCHEM"))) {
                cleanupConnection(conn, rs, null);
                return true;
            }
        }
        cleanupConnection(conn, rs, null);
        return false;
    }

    private boolean isDerby() throws SQLException {
        final Connection conn = getConnectionToDefaultSchema();
        final DatabaseMetaData dbmd = conn.getMetaData();
        return dbmd.getDatabaseProductName().toLowerCase().indexOf("derby") > 0;
    }

    /**
     * Creates the default schema JBATCH or the schema defined in batch-config.
     *
     * @throws SQLException
     */
    private void createSchema() throws SQLException {
        final Connection conn = getConnectionToDefaultSchema();
        final PreparedStatement ps = conn.prepareStatement("CREATE SCHEMA " + schema);
        ps.execute();
        cleanupConnection(conn, null, ps);
    }

    /**
     * Checks if all the runtime batch table exists. If not, it creates them.
     *
     * @throws SQLException
     */
    private void checkAllTables() throws SQLException {
        createIfNotExists(SQLConstants.CHECKPOINTDATA_TABLE, SQLConstants.CREATE_TAB_CHECKPOINTDATA);
        executeStatement(SQLConstants.CREATE_CHECKPOINTDATA_INDEX);
        createIfNotExists(SQLConstants.JOBINSTANCEDATA_TABLE, SQLConstants.CREATE_TAB_JOBINSTANCEDATA);
        createIfNotExists(SQLConstants.EXECUTIONINSTANCEDATA_TABLE, SQLConstants.CREATE_TAB_EXECUTIONINSTANCEDATA);
        createIfNotExists(SQLConstants.STEPEXECUTIONINSTANCEDATA_TABLE, SQLConstants.CREATE_TAB_STEPEXECUTIONINSTANCEDATA);
        createIfNotExists(SQLConstants.JOBSTATUS_TABLE, SQLConstants.CREATE_TAB_JOBSTATUS);
        createIfNotExists(SQLConstants.STEPSTATUS_TABLE, SQLConstants.CREATE_TAB_STEPSTATUS);
    }

    /**
     * Creates tableName using the createTableStatement DDL.
     *
     * @param tableName
     * @param createTableStatement
     * @throws SQLException
     */
    private void createIfNotExists(final String tableName, final String createTableStatement) throws SQLException {
        final Connection conn = getConnection();
        final DatabaseMetaData dbmd = conn.getMetaData();
        final ResultSet rs = dbmd.getTables(null, schema, tableName, null);

        PreparedStatement ps = null;
        if (!rs.next()) {
            ps = conn.prepareStatement(createTableStatement);
            ps.executeUpdate();
        }

        cleanupConnection(conn, rs, ps);
    }

    /**
     * Executes the provided SQL statement
     *
     * @param statement
     * @throws SQLException
     */
    private void executeStatement(final String statement) throws SQLException {
        final Connection conn = getConnection();
        final PreparedStatement ps = conn.prepareStatement(statement);
        ps.executeUpdate();
        cleanupConnection(conn, ps);
    }


    /* (non-Javadoc)
     * @see org.apache.batchee.container.services.impl.AbstractPersistenceManagerImpl#createCheckpointData(com.ibm.ws.batch.container.checkpoint.CheckpointDataKey, com.ibm.ws.batch.container.checkpoint.CheckpointData)
     */
    public void createCheckpointData(final CheckpointDataKey key, final CheckpointData value) {
        insertCheckpointData(key.getCommaSeparatedKey(), value);
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.container.services.impl.AbstractPersistenceManagerImpl#getCheckpointData(com.ibm.ws.batch.container.checkpoint.CheckpointDataKey)
     */
    @Override
    public CheckpointData getCheckpointData(final CheckpointDataKey key) {
        return queryCheckpointData(key.getCommaSeparatedKey());
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.container.services.impl.AbstractPersistenceManagerImpl#setCheckpointData(com.ibm.ws.batch.container.checkpoint.CheckpointDataKey, com.ibm.ws.batch.container.checkpoint.CheckpointData)
     */
    @Override
    public void setCheckpointData(final CheckpointDataKey key, final CheckpointData value) {
        CheckpointData data = queryCheckpointData(key.getCommaSeparatedKey());
        if (data != null) {
            updateCheckpointData(key.getCommaSeparatedKey(), value);
        } else {
            createCheckpointData(key, value);
        }
    }


    /**
     * @return the database connection and sets it to the default schema JBATCH or the schema defined in batch-config.
     * @throws SQLException
     */
    protected Connection getConnection() throws SQLException {
        final Connection connection;
        if (dataSource != null) {
            connection = dataSource.getConnection();
        } else {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                throw new PersistenceException(e);
            }
            connection = DriverManager.getConnection(url, user, pwd);
        }
        setSchemaOnConnection(connection);

        return connection;
    }

    /**
     * @return the database connection. The schema is set to whatever default its used by the underlying database.
     * @throws SQLException
     */
    protected Connection getConnectionToDefaultSchema() throws SQLException {
        final Connection connection;
        if (dataSource != null) {
            try {
                connection = dataSource.getConnection();
            } catch (final SQLException e) {
                throw new PersistenceException(e);
            }
        } else {
            try {
                Class.forName(driver);
            } catch (final ClassNotFoundException e) {
                final StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                throw new PersistenceException(e);
            }
            try {
                connection = DriverManager.getConnection(url, user, pwd);
            } catch (final SQLException e) {
                throw new PersistenceException(e);
            }
        }
        return connection;
    }

    /**
     * Set the default schema JBATCH or the schema defined in batch-config on the connection object.
     *
     * @param connection
     * @throws SQLException
     */
    private void setSchemaOnConnection(final Connection connection) throws SQLException {
        final PreparedStatement ps = connection.prepareStatement("SET SCHEMA ?");
        ps.setString(1, schema);
        ps.executeUpdate();
        ps.close();
    }

    /**
     * select data from DB table
     *
     * @param key - the IPersistenceDataKey object
     * @return List of serializable objects store in the DB table
     * <p/>
     * Ex. select id, obj from tablename where id = ?
     */
    private CheckpointData queryCheckpointData(final Object key) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        CheckpointData data = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(SQLConstants.SELECT_CHECKPOINTDATA);
            statement.setObject(1, key);
            rs = statement.executeQuery();
            if (rs.next()) {
                final byte[] buf = rs.getBytes("obj");
                data = (CheckpointData) deserializeObject(buf);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return data;
    }


    /**
     * insert data to DB table
     *
     * @param key   - the IPersistenceDataKey object
     * @param value - serializable object to store
     *              <p/>
     *              Ex. insert into tablename values(?, ?)
     */
    private <T> void insertCheckpointData(final Object key, final T value) {
        Connection conn = null;
        PreparedStatement statement = null;
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oout = null;
        byte[] b;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(SQLConstants.INSERT_CHECKPOINTDATA);
            baos = new ByteArrayOutputStream();
            oout = new ObjectOutputStream(baos);
            oout.writeObject(value);

            b = baos.toByteArray();

            statement.setObject(1, key);
            statement.setBytes(2, b);
            statement.executeUpdate();
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (final IOException e) {
                    throw new PersistenceException(e);
                }
            }
            if (oout != null) {
                try {
                    oout.close();
                } catch (final IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, null, statement);
        }
    }

    /**
     * update data in DB table
     *
     * @param value - serializable object to store
     * @param key   - the IPersistenceDataKey object
     *              <p/>
     *              Ex. update tablename set obj = ? where id = ?
     */
    private void updateCheckpointData(final Object key, final CheckpointData value) {
        Connection conn = null;
        PreparedStatement statement = null;
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oout = null;
        byte[] b;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(SQLConstants.UPDATE_CHECKPOINTDATA);
            baos = new ByteArrayOutputStream();
            oout = new ObjectOutputStream(baos);
            oout.writeObject(value);

            b = baos.toByteArray();

            statement.setBytes(1, b);
            statement.setObject(2, key);
            statement.executeUpdate();
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            if (oout != null) {
                try {
                    oout.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, null, statement);
        }
    }


    /**
     * closes connection, result set and statement
     *
     * @param conn      - connection object to close
     * @param rs        - result set object to close
     * @param statement - statement object to close
     */
    private void cleanupConnection(final Connection conn, final ResultSet rs, final PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }
    }

    /**
     * closes connection and statement
     *
     * @param conn      - connection object to close
     * @param statement - statement object to close
     */
    private void cleanupConnection(final Connection conn, final PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }
    }


    @Override
    public int jobOperatorGetJobInstanceCount(final String jobName, final String appTag) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        int count;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select count(jobinstanceid) as jobinstancecount from jobinstancedata where name = ? and apptag = ?");
            statement.setString(1, jobName);
            statement.setString(2, appTag);
            rs = statement.executeQuery();
            rs.next();
            count = rs.getInt("jobinstancecount");

        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return count;
    }

    @Override
    public int jobOperatorGetJobInstanceCount(final String jobName) {

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        int count;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(SQLConstants.SELECT_JOBINSTANCEDATA_COUNT);
            statement.setString(1, jobName);
            rs = statement.executeQuery();
            rs.next();
            count = rs.getInt("jobinstancecount");

        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return count;
    }


    @Override
    public List<Long> jobOperatorGetJobInstanceIds(final String jobName, final String appTag, final int start, final int count) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<Long> data = new ArrayList<Long>();

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select jobinstanceid from jobinstancedata where name = ? and apptag = ? order by jobinstanceid desc");
            statement.setObject(1, jobName);
            statement.setObject(2, appTag);
            rs = statement.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("jobinstanceid");
                data.add(id);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        if (data.size() > 0) {
            try {
                return data.subList(start, start + count);
            } catch (IndexOutOfBoundsException oobEx) {
                return data.subList(start, data.size());
            }
        } else return data;
    }

    @Override
    public List<Long> jobOperatorGetJobInstanceIds(final String jobName, final int start, final int count) {

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<Long> data = new ArrayList<Long>();

        try {
            conn = getConnection();
            statement = conn.prepareStatement(SQLConstants.SELECT_JOBINSTANCEDATA_IDS);
            statement.setObject(1, jobName);
            rs = statement.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("jobinstanceid");
                data.add(id);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        if (data.size() > 0) {
            try {
                return data.subList(start, start + count);
            } catch (IndexOutOfBoundsException oobEx) {
                return data.subList(start, data.size());
            }
        } else return data;
    }

    @Override
    public Map<Long, String> jobOperatorGetExternalJobInstanceData() {

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        HashMap<Long, String> data = new HashMap<Long, String>();

        try {
            conn = getConnection();

            // Filter out 'subjob' parallel execution entries which start with the special character
            final String filter = "not like '" + PartitionedStepBuilder.JOB_ID_SEPARATOR + "%'";

            statement = conn.prepareStatement("select distinct jobinstanceid, name from jobinstancedata where name " + filter);
            rs = statement.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("jobinstanceid");
                String name = rs.getString("name");
                data.put(id, name);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return data;
    }

    @Override
    public Timestamp jobOperatorQueryJobExecutionTimestamp(final long key, final TimestampType timestampType) {

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Timestamp createTimestamp = null;
        Timestamp endTimestamp = null;
        Timestamp updateTimestamp = null;
        Timestamp startTimestamp = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select createtime, endtime, updatetime, starttime from executioninstancedata where jobexecid = ?");
            statement.setObject(1, key);
            rs = statement.executeQuery();
            while (rs.next()) {
                createTimestamp = rs.getTimestamp(1);
                endTimestamp = rs.getTimestamp(2);
                updateTimestamp = rs.getTimestamp(3);
                startTimestamp = rs.getTimestamp(4);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        if (timestampType.equals(TimestampType.CREATE)) {
            return createTimestamp;
        } else if (timestampType.equals(TimestampType.END)) {
            return endTimestamp;
        } else if (timestampType.equals(TimestampType.LAST_UPDATED)) {
            return updateTimestamp;
        } else if (timestampType.equals(TimestampType.STARTED)) {
            return startTimestamp;
        } else {
            throw new IllegalArgumentException("Unexpected enum value.");
        }
    }

    @Override
    public String jobOperatorQueryJobExecutionBatchStatus(final long key) {

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        String status = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select batchstatus from executioninstancedata where jobexecid = ?");
            statement.setLong(1, key);
            rs = statement.executeQuery();
            while (rs.next()) {
                status = rs.getString(1);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return status;
    }


    @Override
    public String jobOperatorQueryJobExecutionExitStatus(final long key) {

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        String status = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select exitstatus from executioninstancedata where jobexecid = ?");
            statement.setLong(1, key);
            rs = statement.executeQuery();
            while (rs.next()) {
                status = rs.getString(1);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return status;
    }

    @Override
    public Properties getParameters(final long executionId) throws NoSuchJobExecutionException {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Properties props = null;
        ObjectInputStream objectIn = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select parameters from executioninstancedata where jobexecid = ?");
            statement.setLong(1, executionId);
            rs = statement.executeQuery();

            if (rs.next()) {
                // get the object based data
                byte[] buf = rs.getBytes("parameters");
                props = (Properties) deserializeObject(buf);
            } else {
                throw new NoSuchJobExecutionException("Did not find table entry for executionID =" + executionId);
            }

        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            if (objectIn != null) {
                try {
                    objectIn.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, rs, statement);
        }

        return props;

    }


    public Map<String, StepExecution> getMostRecentStepExecutionsForJobInstance(final long instanceId) {

        final Map<String, StepExecution> data = new HashMap<String, StepExecution>();

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        long jobexecid;
        long stepexecid = 0;
        String stepname;
        String batchstatus;
        String exitstatus;
        long readCount;
        long writeCount;
        long commitCount;
        long rollbackCount;
        long readSkipCount;
        long processSkipCount;
        long filterCount;
        long writeSkipCount;
        Timestamp startTS;
        Timestamp endTS;
        StepExecutionImpl stepEx;
        ObjectInputStream objectIn;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select A.* from stepexecutioninstancedata as A inner join executioninstancedata as B on A.jobexecid = B.jobexecid where B.jobinstanceid = ? order by A.stepexecid desc");
            statement.setLong(1, instanceId);
            rs = statement.executeQuery();
            while (rs.next()) {
                stepname = rs.getString("stepname");
                if (!data.containsKey(stepname)) {
                    jobexecid = rs.getLong("jobexecid");
                    batchstatus = rs.getString("batchstatus");
                    exitstatus = rs.getString("exitstatus");
                    readCount = rs.getLong("readcount");
                    writeCount = rs.getLong("writecount");
                    commitCount = rs.getLong("commitcount");
                    rollbackCount = rs.getLong("rollbackcount");
                    readSkipCount = rs.getLong("readskipcount");
                    processSkipCount = rs.getLong("processskipcount");
                    filterCount = rs.getLong("filtercount");
                    writeSkipCount = rs.getLong("writeSkipCount");
                    startTS = rs.getTimestamp("startTime");
                    endTS = rs.getTimestamp("endTime");
                    // get the object based data
                    Serializable persistentData = null;
                    byte[] pDataBytes = rs.getBytes("persistentData");
                    if (pDataBytes != null) {
                        objectIn = new TCCLObjectInputStream(new ByteArrayInputStream(pDataBytes));
                        persistentData = (Serializable) objectIn.readObject();
                    }

                    stepEx = new StepExecutionImpl(jobexecid, stepexecid);

                    stepEx.setBatchStatus(BatchStatus.valueOf(batchstatus));
                    stepEx.setExitStatus(exitstatus);
                    stepEx.setStepName(stepname);
                    stepEx.setReadCount(readCount);
                    stepEx.setWriteCount(writeCount);
                    stepEx.setCommitCount(commitCount);
                    stepEx.setRollbackCount(rollbackCount);
                    stepEx.setReadSkipCount(readSkipCount);
                    stepEx.setProcessSkipCount(processSkipCount);
                    stepEx.setFilterCount(filterCount);
                    stepEx.setWriteSkipCount(writeSkipCount);
                    stepEx.setStartTime(startTS);
                    stepEx.setEndTime(endTS);
                    stepEx.setPersistentUserData(persistentData);

                    data.put(stepname, stepEx);
                }
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return data;
    }


    @Override
    public List<StepExecution> getStepExecutionsForJobExecution(final long execid) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        long jobexecid;
        long stepexecid;
        String stepname;
        String batchstatus;
        String exitstatus;
        long readCount;
        long writeCount;
        long commitCount;
        long rollbackCount;
        long readSkipCount;
        long processSkipCount;
        long filterCount;
        long writeSkipCount;
        Timestamp startTS;
        Timestamp endTS;
        StepExecutionImpl stepEx;
        ObjectInputStream objectIn;

        final List<StepExecution> data = new ArrayList<StepExecution>();

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select * from stepexecutioninstancedata where jobexecid = ?");
            statement.setLong(1, execid);
            rs = statement.executeQuery();
            while (rs.next()) {
                jobexecid = rs.getLong("jobexecid");
                stepexecid = rs.getLong("stepexecid");
                stepname = rs.getString("stepname");
                batchstatus = rs.getString("batchstatus");
                exitstatus = rs.getString("exitstatus");
                readCount = rs.getLong("readcount");
                writeCount = rs.getLong("writecount");
                commitCount = rs.getLong("commitcount");
                rollbackCount = rs.getLong("rollbackcount");
                readSkipCount = rs.getLong("readskipcount");
                processSkipCount = rs.getLong("processskipcount");
                filterCount = rs.getLong("filtercount");
                writeSkipCount = rs.getLong("writeSkipCount");
                startTS = rs.getTimestamp("startTime");
                endTS = rs.getTimestamp("endTime");
                // get the object based data
                Serializable persistentData = null;
                byte[] pDataBytes = rs.getBytes("persistentData");
                if (pDataBytes != null) {
                    objectIn = new TCCLObjectInputStream(new ByteArrayInputStream(pDataBytes));
                    persistentData = (Serializable) objectIn.readObject();
                }

                stepEx = new StepExecutionImpl(jobexecid, stepexecid);

                stepEx.setBatchStatus(BatchStatus.valueOf(batchstatus));
                stepEx.setExitStatus(exitstatus);
                stepEx.setStepName(stepname);
                stepEx.setReadCount(readCount);
                stepEx.setWriteCount(writeCount);
                stepEx.setCommitCount(commitCount);
                stepEx.setRollbackCount(rollbackCount);
                stepEx.setReadSkipCount(readSkipCount);
                stepEx.setProcessSkipCount(processSkipCount);
                stepEx.setFilterCount(filterCount);
                stepEx.setWriteSkipCount(writeSkipCount);
                stepEx.setStartTime(startTS);
                stepEx.setEndTime(endTS);
                stepEx.setPersistentUserData(persistentData);

                data.add(stepEx);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return data;
    }


    @Override
    public StepExecution getStepExecutionByStepExecutionId(final long stepExecId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        long jobexecid = 0;
        long stepexecid = 0;
        String stepname = null;
        String batchstatus = null;
        String exitstatus = null;
        Exception ex = null;
        long readCount = 0;
        long writeCount = 0;
        long commitCount = 0;
        long rollbackCount = 0;
        long readSkipCount = 0;
        long processSkipCount = 0;
        long filterCount = 0;
        long writeSkipCount = 0;
        Timestamp startTS = null;
        Timestamp endTS = null;
        StepExecutionImpl stepEx = null;
        ObjectInputStream objectIn = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select * from stepexecutioninstancedata where stepexecid = ?");
            statement.setLong(1, stepExecId);
            rs = statement.executeQuery();
            while (rs.next()) {
                jobexecid = rs.getLong("jobexecid");
                stepexecid = rs.getLong("stepexecid");
                stepname = rs.getString("stepname");
                batchstatus = rs.getString("batchstatus");
                exitstatus = rs.getString("exitstatus");
                readCount = rs.getLong("readcount");
                writeCount = rs.getLong("writecount");
                commitCount = rs.getLong("commitcount");
                rollbackCount = rs.getLong("rollbackcount");
                readSkipCount = rs.getLong("readskipcount");
                processSkipCount = rs.getLong("processskipcount");
                filterCount = rs.getLong("filtercount");
                writeSkipCount = rs.getLong("writeSkipCount");
                startTS = rs.getTimestamp("startTime");
                endTS = rs.getTimestamp("endTime");
                // get the object based data
                Serializable persistentData = null;
                byte[] pDataBytes = rs.getBytes("persistentData");
                if (pDataBytes != null) {
                    objectIn = new TCCLObjectInputStream(new ByteArrayInputStream(pDataBytes));
                    persistentData = (Serializable) objectIn.readObject();
                }

                stepEx = new StepExecutionImpl(jobexecid, stepexecid);

                stepEx.setBatchStatus(BatchStatus.valueOf(batchstatus));
                stepEx.setExitStatus(exitstatus);
                stepEx.setStepName(stepname);
                stepEx.setReadCount(readCount);
                stepEx.setWriteCount(writeCount);
                stepEx.setCommitCount(commitCount);
                stepEx.setRollbackCount(rollbackCount);
                stepEx.setReadSkipCount(readSkipCount);
                stepEx.setProcessSkipCount(processSkipCount);
                stepEx.setFilterCount(filterCount);
                stepEx.setWriteSkipCount(writeSkipCount);
                stepEx.setStartTime(startTS);
                stepEx.setEndTime(endTS);
                stepEx.setPersistentUserData(persistentData);


            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } catch (IOException e) {
            throw new PersistenceException(e);
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return stepEx;
    }

    @Override
    public void updateBatchStatusOnly(long key, BatchStatus batchStatus, Timestamp updatets) {
        Connection conn = null;
        PreparedStatement statement = null;
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oout = null;
        byte[] b;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("update executioninstancedata set batchstatus = ?, updatetime = ? where jobexecid = ?");
            statement.setString(1, batchStatus.name());
            statement.setTimestamp(2, updatets);
            statement.setLong(3, key);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PersistenceException(e);
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            if (oout != null) {
                try {
                    oout.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public void updateWithFinalExecutionStatusesAndTimestamps(long key,
                                                              BatchStatus batchStatus, String exitStatus, Timestamp updatets) {
        // TODO Auto-generated methddod stub
        Connection conn = null;
        PreparedStatement statement = null;
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oout = null;
        byte[] b;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("update executioninstancedata set batchstatus = ?, exitstatus = ?, endtime = ?, updatetime = ? where jobexecid = ?");

            statement.setString(1, batchStatus.name());
            statement.setString(2, exitStatus);
            statement.setTimestamp(3, updatets);
            statement.setTimestamp(4, updatets);
            statement.setLong(5, key);

            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PersistenceException(e);
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            if (oout != null) {
                try {
                    oout.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, null, statement);
        }

    }

    public void markJobStarted(long key, Timestamp startTS) {
        Connection conn = null;
        PreparedStatement statement = null;
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oout = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("update executioninstancedata set batchstatus = ?, starttime = ?, updatetime = ? where jobexecid = ?");

            statement.setString(1, BatchStatus.STARTED.name());
            statement.setTimestamp(2, startTS);
            statement.setTimestamp(3, startTS);
            statement.setLong(4, key);

            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PersistenceException(e);
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            if (oout != null) {
                try {
                    oout.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, null, statement);
        }
    }


    @Override
    public InternalJobExecution jobOperatorGetJobExecution(long jobExecutionId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Timestamp createtime;
        Timestamp starttime;
        Timestamp endtime;
        Timestamp updatetime;
        long instanceId;
        String batchStatus;
        String exitStatus;
        JobExecutionImpl jobEx = null;
        ObjectInputStream objectIn = null;
        String jobName;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select A.createtime, A.starttime, A.endtime, A.updatetime, A.parameters, A.jobinstanceid, A.batchstatus, A.exitstatus, B.name from executioninstancedata as A inner join jobinstancedata as B on A.jobinstanceid = B.jobinstanceid where jobexecid = ?");
            statement.setLong(1, jobExecutionId);
            rs = statement.executeQuery();
            while (rs.next()) {
                createtime = rs.getTimestamp("createtime");
                starttime = rs.getTimestamp("starttime");
                endtime = rs.getTimestamp("endtime");
                updatetime = rs.getTimestamp("updatetime");
                instanceId = rs.getLong("jobinstanceid");

                // get the object based data
                batchStatus = rs.getString("batchstatus");
                exitStatus = rs.getString("exitstatus");

                // get the object based data
                Properties params = null;
                byte[] buf = rs.getBytes("parameters");
                params = (Properties) deserializeObject(buf);

                jobName = rs.getString("name");

                jobEx = new JobExecutionImpl(jobExecutionId, instanceId);
                jobEx.setCreateTime(createtime);
                jobEx.setStartTime(starttime);
                jobEx.setEndTime(endtime);
                jobEx.setJobParameters(params);
                jobEx.setLastUpdateTime(updatetime);
                jobEx.setBatchStatus(batchStatus);
                jobEx.setExitStatus(exitStatus);
                jobEx.setJobName(jobName);
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } catch (IOException e) {
            throw new PersistenceException(e);
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            if (objectIn != null) {
                try {
                    objectIn.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, rs, statement);
        }
        return jobEx;
    }

    @Override
    public List<InternalJobExecution> jobOperatorGetJobExecutions(long jobInstanceId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Timestamp createtime = null;
        Timestamp starttime = null;
        Timestamp endtime = null;
        Timestamp updatetime = null;
        long jobExecutionId = 0;
        long instanceId = 0;
        String batchStatus = null;
        String exitStatus = null;
        String jobName = null;
        List<InternalJobExecution> data = new ArrayList<InternalJobExecution>();
        JobExecutionImpl jobEx = null;
        ObjectInputStream objectIn = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select A.jobexecid, A.createtime, A.starttime, A.endtime, A.updatetime, A.parameters, A.batchstatus, A.exitstatus, B.name from executioninstancedata as A inner join jobinstancedata as B ON A.jobinstanceid = B.jobinstanceid where A.jobinstanceid = ?");
            statement.setLong(1, jobInstanceId);
            rs = statement.executeQuery();
            while (rs.next()) {
                jobExecutionId = rs.getLong("jobexecid");
                createtime = rs.getTimestamp("createtime");
                starttime = rs.getTimestamp("starttime");
                endtime = rs.getTimestamp("endtime");
                updatetime = rs.getTimestamp("updatetime");
                batchStatus = rs.getString("batchstatus");
                exitStatus = rs.getString("exitstatus");
                jobName = rs.getString("name");

                // get the object based data
                byte[] buf = rs.getBytes("parameters");
                Properties params = (Properties) deserializeObject(buf);

                jobEx = new JobExecutionImpl(jobExecutionId, instanceId);
                jobEx.setCreateTime(createtime);
                jobEx.setStartTime(starttime);
                jobEx.setEndTime(endtime);
                jobEx.setLastUpdateTime(updatetime);
                jobEx.setBatchStatus(batchStatus);
                jobEx.setExitStatus(exitStatus);
                jobEx.setJobName(jobName);

                data.add(jobEx);
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } catch (IOException e) {
            throw new PersistenceException(e);
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            if (objectIn != null) {
                try {
                    objectIn.close();
                } catch (IOException e) {
                    throw new PersistenceException(e);
                }
            }
            cleanupConnection(conn, rs, statement);
        }
        return data;
    }

    @Override
    public Set<Long> jobOperatorGetRunningExecutions(String jobName) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Set<Long> executionIds = new HashSet<Long>();

        try {
            conn = getConnection();
            statement = conn.prepareStatement("SELECT A.jobexecid FROM executioninstancedata AS A INNER JOIN jobinstancedata AS B ON A.jobinstanceid = B.jobinstanceid WHERE A.batchstatus IN (?,?,?) AND B.name = ?");
            statement.setString(1, BatchStatus.STARTED.name());
            statement.setString(2, BatchStatus.STARTING.name());
            statement.setString(3, BatchStatus.STOPPING.name());
            statement.setString(4, jobName);
            rs = statement.executeQuery();
            while (rs.next()) {
                executionIds.add(rs.getLong("jobexecid"));
            }

        } catch (SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return executionIds;
    }

    @Override
    public JobStatus getJobStatusFromExecution(final long executionId) {

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        JobStatus retVal = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select A.obj from jobstatus as A inner join " +
                "executioninstancedata as B on A.id = B.jobinstanceid where B.jobexecid = ?");
            statement.setLong(1, executionId);
            rs = statement.executeQuery();
            byte[] buf = null;
            if (rs.next()) {
                buf = rs.getBytes("obj");
            }
            retVal = (JobStatus) deserializeObject(buf);
        } catch (Exception e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return retVal;
    }

    public long getJobInstanceIdByExecutionId(final long executionId) throws NoSuchJobExecutionException {
        long instanceId = 0;

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("select jobinstanceid from executioninstancedata where jobexecid = ?");
            statement.setObject(1, executionId);
            rs = statement.executeQuery();
            if (rs.next()) {
                instanceId = rs.getLong("jobinstanceid");
            } else {
                throw new NoSuchJobExecutionException("Did not find job instance associated with executionID =" + executionId);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }

        return instanceId;
    }

    /**
     * This method is used to serialized an object saved into a table BLOB field.
     *
     * @param theObject the object to be serialized
     * @return a object byte array
     * @throws IOException
     */
    private byte[] serializeObject(final Serializable theObject) throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(baos);
        oout.writeObject(theObject);
        byte[] data = baos.toByteArray();
        baos.close();
        oout.close();

        return data;
    }

    /**
     * This method is used to de-serialized a table BLOB field to its original object form.
     *
     * @param buffer the byte array save a BLOB
     * @return the object saved as byte array
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private Serializable deserializeObject(final byte[] buffer) throws IOException, ClassNotFoundException {

        Serializable theObject = null;
        ObjectInputStream objectIn;
        if (buffer != null) {
            objectIn = new ObjectInputStream(new ByteArrayInputStream(buffer));
            theObject = (Serializable) objectIn.readObject();
            objectIn.close();
        }
        return theObject;
    }

    @Override
    public JobInstance createSubJobInstance(final String name, final String apptag) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        JobInstanceImpl jobInstance = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("INSERT INTO jobinstancedata (name, apptag) VALUES(?, ?)", Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, name);
            statement.setString(2, apptag);
            statement.executeUpdate();
            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                final long jobInstanceID = rs.getLong(1);
                jobInstance = new JobInstanceImpl(jobInstanceID);
                jobInstance.setJobName(name);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return jobInstance;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#createJobInstance(java.lang.String, java.lang.String, java.lang.String, java.util.Properties)
     */
    @Override
    public JobInstance createJobInstance(final String name, final String apptag, final String jobXml) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        JobInstanceImpl jobInstance = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement("INSERT INTO jobinstancedata (name, apptag) VALUES(?, ?)", Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, name);
            statement.setString(2, apptag);
            statement.executeUpdate();
            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                long jobInstanceID = rs.getLong(1);
                jobInstance = new JobInstanceImpl(jobInstanceID, jobXml);
                jobInstance.setJobName(name);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return jobInstance;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#createJobExecution(com.ibm.jbatch.container.jsl.JobNavigator, javax.batch.runtime.JobInstance, java.util.Properties, org.apache.batchee.container.impl.JobContextImpl)
     */
    @Override
    public RuntimeJobExecution createJobExecution(final JobInstance jobInstance, final Properties jobParameters, final BatchStatus batchStatus) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        final long newExecutionId = createRuntimeJobExecutionEntry(jobInstance, jobParameters, batchStatus, now);
        final RuntimeJobExecution jobExecution = new RuntimeJobExecution(jobInstance, newExecutionId);
        jobExecution.setBatchStatus(batchStatus.name());
        jobExecution.setCreateTime(now);
        jobExecution.setLastUpdateTime(now);
        return jobExecution;
    }

    private long createRuntimeJobExecutionEntry(final JobInstance jobInstance, final Properties jobParameters, final BatchStatus batchStatus, final Timestamp timestamp) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        long newJobExecutionId = 0L;
        try {
            conn = getConnection();
            statement = conn.prepareStatement("INSERT INTO executioninstancedata (jobinstanceid, createtime, updatetime, batchstatus, parameters) VALUES(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, jobInstance.getInstanceId());
            statement.setTimestamp(2, timestamp);
            statement.setTimestamp(3, timestamp);
            statement.setString(4, batchStatus.name());
            statement.setObject(5, serializeObject(jobParameters));
            statement.executeUpdate();
            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                newJobExecutionId = rs.getLong(1);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return newJobExecutionId;
    }

    @Override
    public RuntimeFlowInSplitExecution createFlowInSplitExecution(final JobInstance jobInstance, final BatchStatus batchStatus) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        final long newExecutionId = createRuntimeJobExecutionEntry(jobInstance, null, batchStatus, now);
        final RuntimeFlowInSplitExecution flowExecution = new RuntimeFlowInSplitExecution(jobInstance, newExecutionId);
        flowExecution.setBatchStatus(batchStatus.name());
        flowExecution.setCreateTime(now);
        flowExecution.setLastUpdateTime(now);
        return flowExecution;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#createStepExecution(long, org.apache.batchee.container.impl.StepContextImpl)
     */
    @Override
    public StepExecutionImpl createStepExecution(final long rootJobExecId, final StepContextImpl stepContext) {
        final String batchStatus = stepContext.getBatchStatus() == null ? BatchStatus.STARTING.name() : stepContext.getBatchStatus().name();
        final String exitStatus = stepContext.getExitStatus();
        final String stepName = stepContext.getStepName();

        long readCount = 0;
        long writeCount = 0;
        long commitCount = 0;
        long rollbackCount = 0;
        long readSkipCount = 0;
        long processSkipCount = 0;
        long filterCount = 0;
        long writeSkipCount = 0;
        Timestamp startTime = stepContext.getStartTimeTS();
        Timestamp endTime = stepContext.getEndTimeTS();

        final Metric[] metrics = stepContext.getMetrics();
        for (final Metric metric : metrics) {
            if (metric.getType().equals(MetricImpl.MetricType.READ_COUNT)) {
                readCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_COUNT)) {
                writeCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.PROCESS_SKIP_COUNT)) {
                processSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.COMMIT_COUNT)) {
                commitCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.ROLLBACK_COUNT)) {
                rollbackCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.READ_SKIP_COUNT)) {
                readSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.FILTER_COUNT)) {
                filterCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_SKIP_COUNT)) {
                writeSkipCount = metric.getValue();
            }
        }
        final Serializable persistentData = stepContext.getPersistentUserData();

        return createStepExecution(rootJobExecId, batchStatus, exitStatus, stepName, readCount,
            writeCount, commitCount, rollbackCount, readSkipCount, processSkipCount, filterCount, writeSkipCount, startTime,
            endTime, persistentData);
    }


    private StepExecutionImpl createStepExecution(final long rootJobExecId, final String batchStatus, final String exitStatus, final String stepName, final long readCount,
                                                  final long writeCount, final long commitCount, final long rollbackCount, final long readSkipCount, final long processSkipCount, final long filterCount,
                                                  final long writeSkipCount, final Timestamp startTime, final Timestamp endTime, final Serializable persistentData) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs;
        StepExecutionImpl stepExecution = null;
        String query = "INSERT INTO stepexecutioninstancedata (jobexecid, batchstatus, exitstatus, stepname, readcount,"
            + "writecount, commitcount, rollbackcount, readskipcount, processskipcount, filtercount, writeskipcount, starttime,"
            + "endtime, persistentdata) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            conn = getConnection();
            statement = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, rootJobExecId);
            statement.setString(2, batchStatus);
            statement.setString(3, exitStatus);
            statement.setString(4, stepName);
            statement.setLong(5, readCount);
            statement.setLong(6, writeCount);
            statement.setLong(7, commitCount);
            statement.setLong(8, rollbackCount);
            statement.setLong(9, readSkipCount);
            statement.setLong(10, processSkipCount);
            statement.setLong(11, filterCount);
            statement.setLong(12, writeSkipCount);
            statement.setTimestamp(13, startTime);
            statement.setTimestamp(14, endTime);
            statement.setObject(15, serializeObject(persistentData));

            statement.executeUpdate();
            rs = statement.getGeneratedKeys();
            if (rs.next()) {
                long stepExecutionId = rs.getLong(1);
                stepExecution = new StepExecutionImpl(rootJobExecId, stepExecutionId);
                stepExecution.setStepName(stepName);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }

        return stepExecution;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#updateStepExecution(long, org.apache.batchee.container.impl.StepContextImpl)
     */
    @Override
    public void updateStepExecution(final long rootJobExecId, final StepContextImpl stepContext) {
        final long stepExecutionId = stepContext.getStepExecutionId();
        final String batchStatus = stepContext.getBatchStatus() == null ? BatchStatus.STARTING.name() : stepContext.getBatchStatus().name();
        final String exitStatus = stepContext.getExitStatus();
        final String stepName = stepContext.getStepName();

        long readCount = 0;
        long writeCount = 0;
        long commitCount = 0;
        long rollbackCount = 0;
        long readSkipCount = 0;
        long processSkipCount = 0;
        long filterCount = 0;
        long writeSkipCount = 0;
        Timestamp startTime = stepContext.getStartTimeTS();
        Timestamp endTime = stepContext.getEndTimeTS();

        Metric[] metrics = stepContext.getMetrics();
        for (final Metric metric : metrics) {
            if (metric.getType().equals(MetricImpl.MetricType.READ_COUNT)) {
                readCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_COUNT)) {
                writeCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.PROCESS_SKIP_COUNT)) {
                processSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.COMMIT_COUNT)) {
                commitCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.ROLLBACK_COUNT)) {
                rollbackCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.READ_SKIP_COUNT)) {
                readSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.FILTER_COUNT)) {
                filterCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_SKIP_COUNT)) {
                writeSkipCount = metric.getValue();
            }
        }
        final Serializable persistentData = stepContext.getPersistentUserData();

        updateStepExecution(stepExecutionId, rootJobExecId, batchStatus, exitStatus, stepName, readCount,
            writeCount, commitCount, rollbackCount, readSkipCount, processSkipCount, filterCount,
            writeSkipCount, startTime, endTime, persistentData);

    }


    private void updateStepExecution(final long stepExecutionId, final long jobExecId, final String batchStatus, final String exitStatus, final String stepName, final long readCount,
                                     final long writeCount, final long commitCount, final long rollbackCount, final long readSkipCount, final long processSkipCount, final long filterCount,
                                     final long writeSkipCount, final Timestamp startTime, final Timestamp endTime, final Serializable persistentData) {
        Connection conn = null;
        PreparedStatement statement = null;
        final String query = "UPDATE stepexecutioninstancedata SET jobexecid = ?, batchstatus = ?, exitstatus = ?, stepname = ?,  readcount = ?,"
            + "writecount = ?, commitcount = ?, rollbackcount = ?, readskipcount = ?, processskipcount = ?, filtercount = ?, writeskipcount = ?,"
            + " starttime = ?, endtime = ?, persistentdata = ? WHERE stepexecid = ?";

        try {
            conn = getConnection();
            statement = conn.prepareStatement(query);
            statement.setLong(1, jobExecId);
            statement.setString(2, batchStatus);
            statement.setString(3, exitStatus);
            statement.setString(4, stepName);
            statement.setLong(5, readCount);
            statement.setLong(6, writeCount);
            statement.setLong(7, commitCount);
            statement.setLong(8, rollbackCount);
            statement.setLong(9, readSkipCount);
            statement.setLong(10, processSkipCount);
            statement.setLong(11, filterCount);
            statement.setLong(12, writeSkipCount);
            statement.setTimestamp(13, startTime);
            statement.setTimestamp(14, endTime);
            statement.setObject(15, serializeObject(persistentData));
            statement.setLong(16, stepExecutionId);
            statement.executeUpdate();
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public JobStatus createJobStatus(final long jobInstanceId) {
        Connection conn = null;
        PreparedStatement statement = null;
        JobStatus jobStatus = new JobStatus(jobInstanceId);
        try {
            conn = getConnection();
            statement = conn.prepareStatement("INSERT INTO jobstatus (id, obj) VALUES(?, ?)");
            statement.setLong(1, jobInstanceId);
            statement.setBytes(2, serializeObject(jobStatus));
            statement.executeUpdate();

        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
        return jobStatus;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#getJobStatus(long)
     */
    @Override
    public JobStatus getJobStatus(final long instanceId) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        String query = "SELECT obj FROM jobstatus WHERE id = ?";
        JobStatus jobStatus = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(query);
            statement.setLong(1, instanceId);
            rs = statement.executeQuery();
            if (rs.next()) {
                jobStatus = (JobStatus) deserializeObject(rs.getBytes(1));
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } catch (final ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return jobStatus;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#updateJobStatus(long, org.apache.batchee.container.status.JobStatus)
     */
    @Override
    public void updateJobStatus(final long instanceId, final JobStatus jobStatus) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement("UPDATE jobstatus SET obj = ? WHERE id = ?");
            statement.setBytes(1, serializeObject(jobStatus));
            statement.setLong(2, instanceId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } catch (IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#createStepStatus(long)
     */
    @Override
    public StepStatus createStepStatus(final long stepExecId) {
        Connection conn = null;
        PreparedStatement statement = null;
        StepStatus stepStatus = new StepStatus(stepExecId);
        try {
            conn = getConnection();
            statement = conn.prepareStatement("INSERT INTO stepstatus (id, obj) VALUES(?, ?)");
            statement.setLong(1, stepExecId);
            statement.setBytes(2, serializeObject(stepStatus));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } catch (IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
        return stepStatus;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#getStepStatus(long, java.lang.String)
     */
    @Override
    public StepStatus getStepStatus(final long instanceId, final String stepName) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        String query = "SELECT obj FROM stepstatus WHERE id IN ("
            + "SELECT B.stepexecid FROM executioninstancedata A INNER JOIN stepexecutioninstancedata B ON A.jobexecid = B.jobexecid "
            + "WHERE A.jobinstanceid = ? and B.stepname = ?)";
        StepStatus stepStatus = null;

        try {
            conn = getConnection();
            statement = conn.prepareStatement(query);
            statement.setLong(1, instanceId);
            statement.setString(2, stepName);
            rs = statement.executeQuery();
            if (rs.next()) {
                stepStatus = (StepStatus) deserializeObject(rs.getBytes(1));
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        } catch (IOException e) {
            throw new PersistenceException(e);
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return stepStatus;
    }

    /* (non-Javadoc)
     * @see org.apache.batchee.spi.PersistenceManagerService#updateStepStatus(long, org.apache.batchee.container.status.StepStatus)
     */
    @Override
    public void updateStepStatus(final long stepExecutionId, final StepStatus stepStatus) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement("UPDATE stepstatus SET obj = ? WHERE id = ?");
            statement.setBytes(1, serializeObject(stepStatus));
            statement.setLong(2, stepExecutionId);
            statement.executeUpdate();
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } catch (final IOException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, null, statement);
        }
    }

    @Override
    public long getMostRecentExecutionId(final long jobInstanceId) {
        long mostRecentId = -1;
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        String query = "SELECT jobexecid FROM executioninstancedata WHERE jobinstanceid = ? ORDER BY createtime DESC";

        try {
            conn = getConnection();
            statement = conn.prepareStatement(query);
            statement.setLong(1, jobInstanceId);
            rs = statement.executeQuery();
            if (rs.next()) {
                mostRecentId = rs.getLong(1);
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        } finally {
            cleanupConnection(conn, rs, statement);
        }
        return mostRecentId;
    }
}
