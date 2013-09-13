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
package org.apache.batchee.test.gui.util;

import org.apache.batchee.util.Batches;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.Properties;

public class CreateSomeJobs implements ServletContextListener {
    @Override
    public void contextInitialized(final ServletContextEvent sce) {
        final JobOperator operator = BatchRuntime.getJobOperator();
        final Properties jobParameters = new Properties();
        jobParameters.setProperty("test", "jbatch");
        final long id = operator.start("init", jobParameters);
        Batches.waitForEnd(operator, id);
    }

    @Override
    public void contextDestroyed(final ServletContextEvent sce) {
        // no-op
    }
}
