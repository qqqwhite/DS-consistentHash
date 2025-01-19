import java.util.ArrayList;
import java.util.List;

/**
 * controller中的文件包装类，简化操作
 *
 */
public class ControllerFileWrapper {
    String fileName;
    int fileSize;
    List<Integer> Dstore;
    DstoreFileStatus status;

    public ControllerFileWrapper(String fileName, int fileSize, List<Integer> dstore, DstoreFileStatus status) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        Dstore = dstore;
        this.status = status;
    }
}
