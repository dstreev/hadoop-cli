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
 * Default credentials implementation that uses the current user's credentials.
 * This is typically used when the user has already authenticated via kinit or
 * when running in a non-secure (simple authentication) environment.
 */
@Slf4j
public class DefaultCredentials implements SessionCredentials {

    /**
     * Returns the UserGroupInformation for the current logged-in user.
     *
     * @return the current user's UserGroupInformation
     * @throws IOException if an error occurs while obtaining the current user
     */
    @Override
    public UserGroupInformation getUGI() throws IOException {
        return UserGroupInformation.getCurrentUser();
    }

    /**
     * Refreshes the credentials by checking the TGT and re-logging from keytab if necessary.
     * This is a no-op if not using keytab-based authentication.
     *
     * @throws IOException if an error occurs while refreshing credentials
     */
    @Override
    public void refresh() throws IOException {
        log.debug("Refreshing default credentials");
        UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
    }
}
