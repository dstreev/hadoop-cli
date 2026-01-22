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

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Interface for managing session credentials in a multi-configuration environment.
 * Implementations provide different authentication strategies including default
 * current user credentials, keytab-based authentication, and proxy user support.
 */
public interface SessionCredentials {

    /**
     * Returns the UserGroupInformation for this session's credentials.
     *
     * @return the UserGroupInformation representing the authenticated user
     * @throws IOException if an error occurs while obtaining credentials
     */
    UserGroupInformation getUGI() throws IOException;

    /**
     * Refreshes the credentials if necessary (e.g., renewing Kerberos tickets).
     *
     * @throws IOException if an error occurs while refreshing credentials
     */
    void refresh() throws IOException;
}
