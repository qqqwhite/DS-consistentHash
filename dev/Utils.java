import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;

/**
 * 工具类
 */
public class Utils {

    public static <T> boolean containsAny(Collection<T> val1, Collection<T> val2){
        for (T element:val2){
            if (val1.contains(element)){
                return true;
            }
        }
        return false;
    }

    public static int getHash(String s, MessageDigest md) {
        int hash = Arrays.hashCode(md.digest(s.getBytes()));
        return Math.abs(hash);
    }


}
