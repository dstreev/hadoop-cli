/*
 * Copyright (c) 2022. David W. Streever All Rights Reserved
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

import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Iterator;

/**
 * Created by streever on 2015-09-29.
 */
public class JCECheck {
    public static void main(final String[] args) {
        final Provider[] providers = Security.getProviders();
        for (int i = 0; i < providers.length; i++) {
            final String name = providers[i].getName();
            final double version = providers[i].getVersion();
            System.out.println("Provider[" + i + "]:: " + name + " " + version);
            if (args.length > 0) {
                final Iterator it = providers[i].keySet().iterator();
                while (it.hasNext()) {
                    final String element = (String) it.next();
                    if (element.toLowerCase().startsWith(args[0].toLowerCase())
                            || args[0].equals("-all"))
                        System.out.println("\t" + element);
                }
            }
        }
        try {
            int keyLength = Cipher.getMaxAllowedKeyLength("AES");
            if (keyLength > 128) {
                System.out.println("JCE is available for Unlimited encryption. ["+keyLength+"]");
            } else {
                System.out.println("JCE is NOT available for Unlimited encryption. ["+keyLength+"]");
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
