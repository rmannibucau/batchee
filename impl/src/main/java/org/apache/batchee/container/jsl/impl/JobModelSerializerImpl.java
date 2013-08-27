/**
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
package org.apache.batchee.container.jsl.impl;

import org.apache.batchee.container.jsl.ModelSerializer;
import org.apache.batchee.jaxb.Contexts;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jsl.util.JSLValidationEventHandler;
import org.apache.batchee.jsl.util.Xsds;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;
import java.io.ByteArrayOutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class JobModelSerializerImpl implements ModelSerializer<JSLJob> {

    @Override
    public String serializeModel(final JSLJob model) {
        return AccessController.doPrivileged(
            new PrivilegedAction<String>() {
                public String run() {
                    return marshalJSLJob(model);
                }
            });
    }

    private String marshalJSLJob(JSLJob job) {
        JSLValidationEventHandler handler = new JSLValidationEventHandler();
        try {
            Marshaller m = Contexts.MODEL.createMarshaller();
            m.setSchema(Xsds.jobXML());
            m.setEventHandler(handler);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            //m.marshal(job, baos);
            /*
    		 * from scott: 
    		 */
            m.marshal(new JAXBElement<JSLJob>(
                new QName("http://xmlns.jcp.org/xml/ns/javaee", "job"), JSLJob.class, job), baos);
            return baos.toString();
        } catch (final Exception e) {
            throw new RuntimeException("Exception while marshalling JSLJob", e);
        }
    }

}
