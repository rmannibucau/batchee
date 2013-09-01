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
package org.apache.batchee.extras.stax;

import org.apache.batchee.extras.checkpoint.Positions;
import org.apache.batchee.extras.stax.util.JAXBContextFactory;
import org.apache.batchee.extras.stax.util.SAXStAXHandler;
import org.apache.batchee.extras.transaction.TransactionalWriter;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import javax.xml.bind.Marshaller;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.sax.SAXResult;
import java.io.File;
import java.io.Serializable;
import java.util.List;

public class StaxItemWriter implements ItemWriter {
    @Inject
    @BatchProperty(name = "marshallingClasses")
    private String marshallingClasses;

    @Inject
    @BatchProperty(name = "marshallingPackage")
    private String marshallingPackage;

    @Inject
    @BatchProperty(name = "rootTag")
    private String rootTag;

    @Inject
    @BatchProperty(name = "encoding")
    private String encoding;

    @Inject
    @BatchProperty(name = "version")
    private String version;

    @Inject
    @BatchProperty(name = "output")
    private String output;

    private Marshaller marshaller;
    private XMLEventWriter writer;
    private XMLEventFactory xmlEventFactory;
    private long position = 0;
    private TransactionalWriter txWriter;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        if (output == null) {
            throw new BatchRuntimeException("output should be set");
        }
        if (marshallingPackage == null && marshallingClasses == null) {
            throw new BatchRuntimeException("marshallingPackage should be set");
        }
        if (encoding == null) {
            encoding = "UTF-8";
        }
        if (rootTag == null) {
            rootTag = "root";
        }
        if (version == null) {
            version = "1.0";
        }

        marshaller = JAXBContextFactory.getJaxbContext(marshallingPackage, marshallingClasses).createMarshaller();
        final File file = new File(output);
        if (!file.getParentFile().exists() && file.getParentFile().mkdirs()) {
            throw new BatchRuntimeException("Output parent file can't be created");
        }

        final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        woodStoxConfig(xmlOutputFactory);

        xmlEventFactory = XMLEventFactory.newFactory();
        txWriter = new TransactionalWriter(file, encoding);
        writer = xmlOutputFactory.createXMLEventWriter(txWriter);

        Positions.reset(txWriter, checkpoint);

        if (position == 0) {
            writer.add(xmlEventFactory.createStartDocument(encoding, version));
            writer.add(xmlEventFactory.createStartElement("", "", rootTag));
            writer.flush();
        }
    }

    // this config is mainly taken from spring-batch and cxf
    private void woodStoxConfig(final XMLOutputFactory xmlOutputFactory) {
        if (xmlOutputFactory.isPropertySupported("com.ctc.wstx.automaticEndElements")) {
            xmlOutputFactory.setProperty("com.ctc.wstx.automaticEndElements", Boolean.FALSE);
        }
        if (xmlOutputFactory.isPropertySupported("com.ctc.wstx.outputValidateStructure")) {
            xmlOutputFactory.setProperty("com.ctc.wstx.outputValidateStructure", Boolean.FALSE);
        }
    }

    @Override
    public void close() throws Exception {
        writer.add(xmlEventFactory.createEndElement("", "", rootTag));
        writer.add(xmlEventFactory.createEndDocument());
        writer.close();
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object item : items) {
            marshaller.marshal(item, new SAXResult(new SAXStAXHandler(writer, xmlEventFactory)));
        }
        writer.flush();
        position = txWriter.position();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return position;
    }
}
