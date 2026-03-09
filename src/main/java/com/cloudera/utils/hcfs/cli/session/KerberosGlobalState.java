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

package com.cloudera.utils.hcfs.cli.session;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Manages global Kerberos state initialization for UserGroupInformation.
 *
 * <p>This class ensures that {@link UserGroupInformation#setConfiguration} is called
 * to enable Kerberos authentication globally. This is required because
 * {@link UserGroupInformation#loginUserFromKeytabAndReturnUGI} checks
 * {@link UserGroupInformation#isSecurityEnabled()} which depends on the global configuration.
 *
 * <p><b>Why is this needed?</b>
 * <ul>
 *   <li>{@code setConfiguration()} modifies GLOBAL JVM state</li>
 *   <li>Multiple sessions calling it with different configs will interfere</li>
 *   <li>But Kerberos must be enabled globally for keytab authentication to work</li>
 *   <li>Solution: Check actual UGI state, initialize only when needed, use isolated UGIs per session</li>
 * </ul>
 *
 * <p><b>Recovery behavior:</b> Unlike the previous "initialize-once" approach, this class
 * checks {@link UserGroupInformation#isSecurityEnabled()} (the actual state) rather than
 * a static flag. If something resets UGI state (e.g., another component, plugin reinstall,
 * classloader recycling), the next call will detect the reset and re-initialize.
 *
 * <p><b>Usage pattern:</b>
 * <pre>{@code
 * // In session initialization:
 * if (kerberosEnabled) {
 *     KerberosGlobalState.ensureInitialized();  // Safe to call multiple times
 *     UserGroupInformation ugi = credentials.getUGI();  // Returns isolated UGI
 *     ugi.doAs(() -> { ... });
 * }
 * }</pre>
 */
@Slf4j
public final class KerberosGlobalState {

    private static final Object lock = new Object();

    private KerberosGlobalState() {
        // Utility class - no instantiation
    }

    /**
     * Ensures that Kerberos is enabled in the global UserGroupInformation configuration.
     * This method is idempotent and self-healing — safe to call multiple times from
     * multiple threads.
     *
     * <p>Checks {@link UserGroupInformation#isSecurityEnabled()} to determine if
     * initialization is needed. This means:
     * <ul>
     *   <li>If UGI is already configured for Kerberos, this is a no-op (fast path)</li>
     *   <li>If something reset UGI state, this will re-initialize it</li>
     *   <li>Plugin reinstalls (new classloader) get a fresh UGI that needs initialization</li>
     * </ul>
     */
    public static void ensureInitialized() {
        if (UserGroupInformation.isSecurityEnabled()) {
            return;
        }

        synchronized (lock) {
            if (UserGroupInformation.isSecurityEnabled()) {
                return;
            }

            Configuration kerberosConfig = new Configuration(false);
            kerberosConfig.set("hadoop.security.authentication", "kerberos");

            log.info("Initializing global Kerberos configuration for UserGroupInformation");
            UserGroupInformation.setConfiguration(kerberosConfig);
        }
    }

    /**
     * Ensures Kerberos is initialized using the provided configuration.
     * This variant allows setting additional Kerberos-related properties
     * (e.g., auth_to_local rules) on the first initialization.
     *
     * <p>If UGI is already configured for Kerberos, this is a no-op.
     * Only use this when you need specific configuration values beyond
     * the minimal {@code hadoop.security.authentication=kerberos}.
     *
     * @param config the Configuration to use for Kerberos initialization
     */
    public static void ensureInitialized(Configuration config) {
        if (UserGroupInformation.isSecurityEnabled()) {
            return;
        }

        synchronized (lock) {
            if (UserGroupInformation.isSecurityEnabled()) {
                return;
            }

            String authMode = config.get("hadoop.security.authentication", "simple");
            if (!"kerberos".equalsIgnoreCase(authMode)) {
                log.debug("Configuration does not have Kerberos enabled, skipping global init");
                return;
            }

            log.info("Initializing global Kerberos configuration from provided Configuration");
            UserGroupInformation.setConfiguration(config);
        }
    }

    /**
     * Checks if global Kerberos state is currently active.
     *
     * @return true if Kerberos authentication is currently enabled in UGI
     */
    public static boolean isInitialized() {
        return UserGroupInformation.isSecurityEnabled();
    }
}
