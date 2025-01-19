import java.io.IOException;

public class DstoreTest2 {
    public static void main(String[] args) throws IOException {
        Dstore dstore =new Dstore(8002, 8000, Config.timeout, "file_folder_8002");
    }
}
