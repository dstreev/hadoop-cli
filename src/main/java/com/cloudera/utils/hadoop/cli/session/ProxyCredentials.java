/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.cloudera.utils.hadoop.cli.session;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Credentials implementation that creates a proxy user based on real credentials.
 * This allows operations to be performed on behalf of another user while using
 * the authentication of the real credentials.
 */
@Slf4j
public class ProxyCredentials implements SessionCredentials {

    private final String proxyUser;
    private final SessionCredentials realCredentials;
    private UserGroupInformation proxyUgi;

    /**
     * Creates a new ProxyCredentials instance.
     *
     * @param proxyUser        the username to impersonate
     * @param realCredentials  the actual credentials used for authentication
     */
    public ProxyCredentials(String proxyUser, SessionCredentials realCredentials) {
        this.proxyUser = proxyUser;
        this.realCredentials = realCredentials;
    }

    /**
     * Returns the UserGroupInformation for the proxy user.
     * The proxy UGI is lazily initialized on first call.
     * This method is thread-safe.
     *
     * @return the UserGroupInformation representing the proxy user
     * @throws IOException if an error occurs while creating the proxy user
     */
    @Override
    public synchronized UserGroupInformation getUGI() throws IOException {
        if (proxyUgi == null) {
            UserGroupInformation realUgi = realCredentials.getUGI();
            log.info("Creating proxy user: {} with real user: {}", proxyUser, realUgi.getUserName());
            proxyUgi = UserGroupInformation.createProxyUser(proxyUser, realUgi);
            log.debug("Successfully created proxy UGI for {}", proxyUser);
        }
        return proxyUgi;
    }

    /**
     * Refreshes the underlying real credentials.
     * The proxy UGI is invalidated and will be recreated on next getUGI() call.
     *
     * @throws IOException if an error occurs while refreshing the real credentials
     */
    @Override
    public synchronized void refresh() throws IOException {
        log.debug("Refreshing proxy credentials for user: {}", proxyUser);
        realCredentials.refresh();
        // Invalidate the proxy UGI so it gets recreated with refreshed real credentials
        proxyUgi = null;
    }
}
