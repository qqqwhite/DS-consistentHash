import java.io.IOException;

public class DstoreTest3 {
    public static void main(String[] args) throws IOException {
        Dstore dstore =new Dstore(8003, 8000, Config.timeout, "file_folder_8003");
    }
}
