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
package org.apache.batchee.container.modelresolver;


import org.apache.batchee.container.jsl.TransitionElement;
import org.apache.batchee.container.modelresolver.impl.AnalyzerPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.BatchletPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.CheckpointAlgorithmPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.ChunkPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.CollectorPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.ControlElementPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.DecisionPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.ExceptionClassesPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.FlowPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.ItemProcessorPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.ItemReaderPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.ItemWriterPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.JobPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.ListenerPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.PartitionMapperPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.PartitionPlanPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.PartitionPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.PartitionReducerPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.SplitPropertyResolverImpl;
import org.apache.batchee.container.modelresolver.impl.StepPropertyResolverImpl;
import org.apache.batchee.jaxb.Analyzer;
import org.apache.batchee.jaxb.Batchlet;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.Collector;
import org.apache.batchee.jaxb.Decision;
import org.apache.batchee.jaxb.ExceptionClassFilter;
import org.apache.batchee.jaxb.Flow;
import org.apache.batchee.jaxb.ItemProcessor;
import org.apache.batchee.jaxb.ItemReader;
import org.apache.batchee.jaxb.ItemWriter;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.Listener;
import org.apache.batchee.jaxb.Partition;
import org.apache.batchee.jaxb.PartitionMapper;
import org.apache.batchee.jaxb.PartitionPlan;
import org.apache.batchee.jaxb.PartitionReducer;
import org.apache.batchee.jaxb.Split;
import org.apache.batchee.jaxb.Step;

public class PropertyResolverFactory {


    public static PropertyResolver<JSLJob> createJobPropertyResolver(boolean isPartitionedStep) {
        return new JobPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Step> createStepPropertyResolver(boolean isPartitionedStep) {
        return new StepPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Batchlet> createBatchletPropertyResolver(boolean isPartitionedStep) {
        return new BatchletPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Split> createSplitPropertyResolver(boolean isPartitionedStep) {
        return new SplitPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Flow> createFlowPropertyResolver(boolean isPartitionedStep) {
        return new FlowPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Chunk> createChunkPropertyResolver(boolean isPartitionedStep) {
        return new ChunkPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<TransitionElement> createTransitionElementPropertyResolver(boolean isPartitionedStep) {
        return new ControlElementPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Decision> createDecisionPropertyResolver(boolean isPartitionedStep) {
        return new DecisionPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Listener> createListenerPropertyResolver(boolean isPartitionedStep) {
        return new ListenerPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Partition> createPartitionPropertyResolver(boolean isPartitionedStep) {
        return new PartitionPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<PartitionMapper> createPartitionMapperPropertyResolver(boolean isPartitionedStep) {
        return new PartitionMapperPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<PartitionPlan> createPartitionPlanPropertyResolver(boolean isPartitionedStep) {
        return new PartitionPlanPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<PartitionReducer> createPartitionReducerPropertyResolver(boolean isPartitionedStep) {
        return new PartitionReducerPropertyResolverImpl(isPartitionedStep);
    }

    public static CheckpointAlgorithmPropertyResolverImpl createCheckpointAlgorithmPropertyResolver(boolean isPartitionedStep) {
        return new CheckpointAlgorithmPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Collector> createCollectorPropertyResolver(boolean isPartitionedStep) {
        return new CollectorPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<Analyzer> createAnalyzerPropertyResolver(boolean isPartitionedStep) {
        return new AnalyzerPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<ItemReader> createReaderPropertyResolver(boolean isPartitionedStep) {
        return new ItemReaderPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<ItemProcessor> createProcessorPropertyResolver(boolean isPartitionedStep) {
        return new ItemProcessorPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<ItemWriter> createWriterPropertyResolver(boolean isPartitionedStep) {
        return new ItemWriterPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<ExceptionClassFilter> createSkippableExceptionClassesPropertyResolver(boolean isPartitionedStep) {
        return new ExceptionClassesPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<ExceptionClassFilter> createRetryableExceptionClassesPropertyResolver(boolean isPartitionedStep) {
        return new ExceptionClassesPropertyResolverImpl(isPartitionedStep);
    }

    public static PropertyResolver<ExceptionClassFilter> createNoRollbackExceptionClassesPropertyResolver(boolean isPartitionedStep) {
        return new ExceptionClassesPropertyResolverImpl(isPartitionedStep);
    }

}
