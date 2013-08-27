package org.apache.batchee.jaxb;

import javax.batch.operations.BatchRuntimeException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

public final class Contexts {
    public static final JAXBContext MODEL = modelContext();

    private static JAXBContext modelContext() {
        try {
            return JAXBContext.newInstance(JSLJob.class.getPackage().getName());
        } catch (final JAXBException e) {
            throw new BatchRuntimeException(e);
        }
    }

    private Contexts() {
        // no-op
    }
}
