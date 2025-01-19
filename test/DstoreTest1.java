import java.io.IOException;

public class DstoreTest1 {
    public static void main(String[] args) throws IOException {
        Dstore dstore =new Dstore(8001, 8000, Config.timeout, "file_folder_8001");
    }
}
