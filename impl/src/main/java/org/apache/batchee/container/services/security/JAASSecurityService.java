package org.apache.batchee.container.services.security;

import javax.security.auth.Subject;
import java.security.AccessController;
import java.security.Principal;
import java.util.Iterator;
import java.util.Properties;

public class JAASSecurityService extends DefaultSecurityService {
    private static boolean isAuthenticatedAndAuthorized(final String permission) {
        final Subject subject = getSubject();
        if (subject == null) {
            return false;
        }
        for (final BatchRole role : subject.getPrincipals(BatchRole.class)) {
            if (role.getName().equals(permission)) {
                return true;
            }
        }
        return false;
    }

    private static Subject getSubject() {
        return Subject.getSubject(AccessController.getContext());
    }

    @Override
    public boolean isAuthorized(final long instanceId) {
        return isAuthenticatedAndAuthorized("update");
    }

    @Override
    public boolean isAuthorized(final String perm) {
        return isAuthenticatedAndAuthorized(perm);
    }

    @Override
    public String getLoggedUser() {
        final Subject subject = getSubject();
        if (subject != null) {
            final Iterator<BatchUser> iterator = subject.getPrincipals(BatchUser.class).iterator();
            if (iterator.hasNext()) {
                return iterator.next().getName();
            }
        }
        return super.getLoggedUser();
    }

    @Override
    public void init(final Properties batchConfig) {
        super.init(batchConfig);
    }

    public static abstract class BatchPrincipal implements Principal {
        private final String name;

        public BatchPrincipal(final String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    public static class BatchUser extends BatchPrincipal {
        public BatchUser(final String name) {
            super(name);
        }
    }

    public static class BatchRole extends BatchPrincipal {
        public BatchRole(final String name) {
            super(name);
        }
    }
}
