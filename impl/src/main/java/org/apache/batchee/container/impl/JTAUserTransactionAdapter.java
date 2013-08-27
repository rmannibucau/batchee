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
package org.apache.batchee.container.impl;

import org.apache.batchee.container.exception.TransactionManagementException;
import org.apache.batchee.spi.services.TransactionManagerAdapter;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

public class JTAUserTransactionAdapter implements TransactionManagerAdapter {
    protected UserTransaction userTran = null;

    public JTAUserTransactionAdapter(final String jndiLookup) {
        try {
            userTran = UserTransaction.class.cast(new InitialContext().lookup(jndiLookup));
        } catch (final NamingException ne) {
            throw new TransactionManagementException(ne);
        }
    }

    @Override
    public void begin() throws TransactionManagementException {
        try {
            userTran.begin();
        } catch (final NotSupportedException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void commit() throws TransactionManagementException {
        try {
            userTran.commit();
        } catch (final SecurityException e) {
            throw new TransactionManagementException(e);
        } catch (final IllegalStateException e) {
            throw new TransactionManagementException(e);
        } catch (final RollbackException e) {
            throw new TransactionManagementException(e);
        } catch (final HeuristicMixedException e) {
            throw new TransactionManagementException(e);
        } catch (final HeuristicRollbackException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void rollback() throws TransactionManagementException {
        try {
            userTran.rollback();
        } catch (final IllegalStateException e) {
            throw new TransactionManagementException(e);
        } catch (final SecurityException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public int getStatus() throws TransactionManagementException {
        try {
            return userTran.getStatus();
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void setRollbackOnly() throws TransactionManagementException {
        try {
            userTran.setRollbackOnly();
        } catch (final IllegalStateException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void setTransactionTimeout(final int seconds) throws TransactionManagementException {
        try {
            userTran.setTransactionTimeout(seconds);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }
}
