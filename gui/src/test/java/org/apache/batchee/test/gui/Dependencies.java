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
package org.apache.batchee.test.gui;

import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.PomEquippedResolveStage;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

public class Dependencies {
    private static File[] RESOLVED_DEPENDENCIES = null;

    public static File[] get() {
        if (RESOLVED_DEPENDENCIES  != null) {
            return RESOLVED_DEPENDENCIES;
        }

        final PomEquippedResolveStage equippedResolveStage = Maven.resolver()
            .offline().loadPomFromFile("pom.xml");

        final Collection<File> files = new LinkedList<File>();

        files.addAll(Arrays.asList(
            equippedResolveStage
                .importCompileAndRuntimeDependencies()
                .resolve().withTransitivity().asFile()));

        files.add(equippedResolveStage // scope test but we don't want others
            .resolve("org.apache.batchee:batchee-jbatch")
            .withoutTransitivity().asSingleFile());

        files.add(equippedResolveStage // scope provided but we need it for tests
            .resolve("javax.batch:javax.batch-api")
            .withoutTransitivity().asSingleFile());

        try {
            RESOLVED_DEPENDENCIES = files.toArray(new File[files.size()]);
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
        return RESOLVED_DEPENDENCIES;
    }
}
