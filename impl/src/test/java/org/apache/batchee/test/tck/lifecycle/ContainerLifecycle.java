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
package org.apache.batchee.test.tck.lifecycle;

import org.apache.derby.jdbc.EmbeddedDriver;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import javax.batch.operations.BatchRuntimeException;
import javax.ejb.embeddable.EJBContainer;
import java.util.Properties;

// forces the execution in embedded container
public class ContainerLifecycle implements ITestListener {
    private EJBContainer container;

    @Override
    public void onTestStart(final ITestResult iTestResult) {
        System.out.println("====================================================================================================");
        System.out.println(iTestResult.getMethod().getRealClass().getName() + "#" + iTestResult.getMethod().getMethodName());
        System.out.println("----------------------------------------------------------------------------------------------------");
        System.out.println("");
    }

    @Override
    public void onTestSuccess(final ITestResult iTestResult) {
        System.out.println(">>> SUCCESS");
    }

    @Override
    public void onTestFailure(final ITestResult iTestResult) {
        System.out.println(">>> FAILURE");
    }

    @Override
    public void onTestSkipped(final ITestResult iTestResult) {
        System.out.println(">>> SKIPPED");
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(final ITestResult iTestResult) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onStart(final ITestContext iTestContext) {
        container = EJBContainer.createEJBContainer(defineDS(defineDS(new Properties(), "orderDB", true), "batch", false));
    }

    @Override
    public void onFinish(final ITestContext iTestContext) {
        if (container != null) {
            try {
                container.close();
            } catch (final Exception e) {
                throw new BatchRuntimeException(e);
            }
        }
    }

    private static Properties defineDS(final Properties p, final String name, final boolean jta) {
        p.setProperty("jdbc/" + name, "new://Resource?type=DataSource");
        p.setProperty("jdbc/" + name + ".JdbcDriver", EmbeddedDriver.class.getName());
        p.setProperty("jdbc/" + name + ".JdbcUrl", "jdbc:derby:memory:" + name + ";create=true");
        p.setProperty("jdbc/" + name + ".UserName", "app");
        p.setProperty("jdbc/" + name + ".Password", "app");
        p.setProperty("jdbc/" + name + ".JtaManaged", Boolean.toString(jta));
        return p;
    }
}
