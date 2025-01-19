import java.io.IOException;

public class DstoreTest4 {
    public static void main(String[] args) throws IOException {
        Dstore dstore =new Dstore(8004, 8000, Config.timeout, "file_folder_8004");
    }
}
