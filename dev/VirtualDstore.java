import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 虚拟节点
 */
public class VirtualDstore {
    private static int count;
    private final int id;
    public int dPort;
    List<String> files = new CopyOnWriteArrayList<>();

    private VirtualDstore() {
        id = count;
        count++;
    }

    public static VirtualDstore newInstance(){
        return new VirtualDstore();
    }
}
