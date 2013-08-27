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
package org.apache.batchee.container.proxy;

import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.servicesmanager.ServicesManagerImpl;
import org.apache.batchee.container.validation.ArtifactValidationException;
import org.apache.batchee.spi.services.IBatchArtifactFactory;

import javax.batch.api.Batchlet;
import javax.batch.api.Decider;
import javax.batch.api.chunk.CheckpointAlgorithm;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.api.partition.PartitionAnalyzer;
import javax.batch.api.partition.PartitionCollector;
import javax.batch.api.partition.PartitionMapper;
import javax.batch.api.partition.PartitionReducer;

/*
 * Introduce a level of indirection so proxies are not instantiated directly by newing them up.
 */
public class ProxyFactory {
    private static IBatchArtifactFactory batchArtifactFactory = ServicesManagerImpl.getInstance().getDelegatingArtifactFactory();

    private static ThreadLocal<InjectionReferences> injectionContext = new ThreadLocal<InjectionReferences>();

    protected static Object loadArtifact(final String id, final InjectionReferences injectionReferences) {
        injectionContext.set(injectionReferences);
        try {
            return batchArtifactFactory.load(id);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static InjectionReferences getInjectionReferences() {
        return injectionContext.get();
    }

    /*
     * Decider
     */
    public static DeciderProxy createDeciderProxy(String id, InjectionReferences injectionRefs) throws ArtifactValidationException {
        return new DeciderProxy(Decider.class.cast(loadArtifact(id, injectionRefs)));
    }

    /*
     * Batchlet artifact
     */
    public static BatchletProxy createBatchletProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final Batchlet loadedArtifact = (Batchlet) loadArtifact(id, injectionRefs);
        final BatchletProxy proxy = new BatchletProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }
    
    /*
     * The four main chunk-related artifacts
     */

    public static CheckpointAlgorithmProxy createCheckpointAlgorithmProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final CheckpointAlgorithm loadedArtifact = (CheckpointAlgorithm) loadArtifact(id, injectionRefs);
        final CheckpointAlgorithmProxy proxy = new CheckpointAlgorithmProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static ItemReaderProxy createItemReaderProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final ItemReader loadedArtifact = (ItemReader) loadArtifact(id, injectionRefs);
        final ItemReaderProxy proxy = new ItemReaderProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static ItemProcessorProxy createItemProcessorProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final ItemProcessor loadedArtifact = (ItemProcessor) loadArtifact(id, injectionRefs);
        final ItemProcessorProxy proxy = new ItemProcessorProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static ItemWriterProxy createItemWriterProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final ItemWriter loadedArtifact = (ItemWriter) loadArtifact(id, injectionRefs);
        final ItemWriterProxy proxy = new ItemWriterProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }
        
    /*
     * The four partition-related artifacts
     */

    public static PartitionReducerProxy createPartitionReducerProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final PartitionReducer loadedArtifact = (PartitionReducer) loadArtifact(id, injectionRefs);
        final PartitionReducerProxy proxy = new PartitionReducerProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static PartitionMapperProxy createPartitionMapperProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final PartitionMapper loadedArtifact = (PartitionMapper) loadArtifact(id, injectionRefs);
        final PartitionMapperProxy proxy = new PartitionMapperProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static PartitionAnalyzerProxy createPartitionAnalyzerProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final PartitionAnalyzer loadedArtifact = (PartitionAnalyzer) loadArtifact(id, injectionRefs);
        final PartitionAnalyzerProxy proxy = new PartitionAnalyzerProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static PartitionCollectorProxy createPartitionCollectorProxy(String id, InjectionReferences injectionRefs, StepContextImpl stepContext) throws ArtifactValidationException {
        final PartitionCollector loadedArtifact = (PartitionCollector) loadArtifact(id, injectionRefs);
        final PartitionCollectorProxy proxy = new PartitionCollectorProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }
}
