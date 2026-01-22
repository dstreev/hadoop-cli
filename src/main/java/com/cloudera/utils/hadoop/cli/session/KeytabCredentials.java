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
 * Credentials implementation that authenticates using a Kerberos keytab file.
 * The UGI is lazily initialized on first access and is thread-safe.
 */
@Slf4j
public class KeytabCredentials implements SessionCredentials {

    private final String principal;
    private final String keytabPath;
    private UserGroupInformation ugi;

    /**
     * Creates a new KeytabCredentials instance.
     *
     * @param principal   the Kerberos principal to authenticate as
     * @param keytabPath  the path to the keytab file containing the principal's credentials
     */
    public KeytabCredentials(String principal, String keytabPath) {
        this.principal = principal;
        this.keytabPath = keytabPath;
    }

    /**
     * Returns the UserGroupInformation for the keytab-authenticated user.
     * The UGI is lazily initialized on first call and cached for subsequent calls.
     * This method is thread-safe.
     *
     * @return the UserGroupInformation for the authenticated principal
     * @throws IOException if an error occurs during authentication
     */
    @Override
    public synchronized UserGroupInformation getUGI() throws IOException {
        if (ugi == null) {
            log.info("Logging in from keytab: principal={}, keytab={}", principal, keytabPath);
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
            log.debug("Successfully authenticated as {}", ugi.getUserName());
        }
        return ugi;
    }

    /**
     * Refreshes the credentials by re-logging from the keytab.
     * This method is thread-safe.
     *
     * @throws IOException if an error occurs while refreshing credentials
     */
    @Override
    public synchronized void refresh() throws IOException {
        if (ugi != null) {
            log.debug("Refreshing keytab credentials for principal: {}", principal);
            ugi.checkTGTAndReloginFromKeytab();
        } else {
            log.debug("UGI not initialized, performing initial login");
            getUGI();
        }
    }
}
