import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Iterator;

/**
 * Created by dstreev on 2015-09-29.
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
