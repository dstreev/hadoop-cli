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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Manages global Kerberos state initialization for UserGroupInformation.
 *
 * <p>This class ensures that {@link UserGroupInformation#setConfiguration} is called
 * exactly ONCE to enable Kerberos authentication globally. This is required because
 * {@link UserGroupInformation#loginUserFromKeytabAndReturnUGI} checks
 * {@link UserGroupInformation#isSecurityEnabled()} which depends on the global configuration.
 *
 * <p><b>Why is this needed?</b>
 * <ul>
 *   <li>{@code setConfiguration()} modifies GLOBAL JVM state</li>
 *   <li>Multiple sessions calling it with different configs will interfere</li>
 *   <li>But Kerberos must be enabled globally for keytab authentication to work</li>
 *   <li>Solution: Initialize once, then use isolated UGIs per session</li>
 * </ul>
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

    private static volatile boolean initialized = false;
    private static final Object lock = new Object();

    private KerberosGlobalState() {
        // Utility class - no instantiation
    }

    /**
     * Ensures that Kerberos is enabled in the global UserGroupInformation configuration.
     * This method is idempotent - safe to call multiple times from multiple threads.
     *
     * <p>Only the FIRST call will actually call {@code setConfiguration()}. Subsequent
     * calls will return immediately without modifying global state.
     */
    public static void ensureInitialized() {
        if (initialized) {
            return;
        }

        synchronized (lock) {
            if (initialized) {
                return;
            }

            Configuration kerberosConfig = new Configuration(false);
            kerberosConfig.set("hadoop.security.authentication", "kerberos");

            log.info("Initializing global Kerberos configuration for UserGroupInformation (one-time)");
            UserGroupInformation.setConfiguration(kerberosConfig);
            initialized = true;
        }
    }

    /**
     * Ensures Kerberos is initialized using the provided configuration.
     * This variant allows setting additional Kerberos-related properties.
     *
     * <p>Note: Only the FIRST call will take effect. If you need specific
     * configuration values, call this method early in application startup.
     *
     * @param config the Configuration to use for Kerberos initialization
     */
    public static void ensureInitialized(Configuration config) {
        if (initialized) {
            log.debug("Kerberos already initialized, ignoring additional configuration");
            return;
        }

        synchronized (lock) {
            if (initialized) {
                log.debug("Kerberos already initialized, ignoring additional configuration");
                return;
            }

            String authMode = config.get("hadoop.security.authentication", "simple");
            if (!"kerberos".equalsIgnoreCase(authMode)) {
                log.debug("Configuration does not have Kerberos enabled, skipping global init");
                return;
            }

            log.info("Initializing global Kerberos configuration from provided Configuration (one-time)");
            UserGroupInformation.setConfiguration(config);
            initialized = true;
        }
    }

    /**
     * Checks if global Kerberos state has been initialized.
     *
     * @return true if {@link #ensureInitialized()} has been called successfully
     */
    public static boolean isInitialized() {
        return initialized;
    }

    /**
     * Resets the initialization state. This should only be used in tests.
     * In production, Kerberos state should be initialized once and never reset.
     */
    public static void resetForTesting() {
        synchronized (lock) {
            initialized = false;
            log.warn("Kerberos global state reset - this should only be used in tests!");
        }
    }
}
