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
package org.apache.batchee.container.jsl;

import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;
import java.util.logging.Logger;

public class JSLValidationEventHandler implements ValidationEventHandler {
    private final static Logger logger = Logger.getLogger(JSLValidationEventHandler.class.getName());

    private boolean eventOccurred = false;

    public boolean handleEvent(final ValidationEvent event) {
        logger.warning("JSL invalid per XSD, details: " + ("\nMESSAGE: " + event.getMessage()) + "\nSEVERITY: " + event.getSeverity() + "\nLINKED EXC: " + event.getLinkedException() + "\nLOCATOR INFO:\n------------" + "\n  COLUMN NUMBER:  " + event.getLocator().getColumnNumber() + "\n  LINE NUMBER:  " + event.getLocator().getLineNumber() + "\n  OFFSET:  " + event.getLocator().getOffset() + "\n  CLASS:  " + event.getLocator().getClass() + "\n  NODE:  " + event.getLocator().getNode() + "\n  OBJECT:  " + event.getLocator().getObject() + "\n  URL:  " + event.getLocator().getURL());

        eventOccurred = true;

        // Allow more parsing feedback 
        return true;
    }

    public boolean eventOccurred() {
        return eventOccurred;
    }
}
