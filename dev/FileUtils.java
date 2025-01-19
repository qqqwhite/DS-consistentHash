import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 文件相关工具类，简化操作，提高代码复用率
 */
public class FileUtils {
    public static void createFolder(String folderName) throws IOException {
        Path folderPath = Paths.get(folderName);
        if (!Files.isDirectory(folderPath)) {
            Files.createDirectory(folderPath);
        }
        clearFolder(new File(folderName));
    }

    public static void clearFolder(File folder) {
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        clearFolder(file);
                    } else {
                        file.delete();
                    }
                }
            }
        }
    }

    public static void storeFile(String folder, String filename, byte[] fileContent) throws IOException {
        String filePath = folder + "/" + filename;
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        file = null;
        FileOutputStream fos = new FileOutputStream(filePath);
        fos.write(fileContent);
        fos.close();
    }

    public static byte[] loadFile(String folder, String filename) throws IOException {
        Path filePath = Paths.get(folder + "/" + filename);
        byte[] fileContent = Files.readAllBytes(filePath);
        return fileContent;
    }

    public static boolean isExist(String folder, String filename) {
        String filePath = folder + "/" + filename;
        File file = new File(filePath);
        return file.exists();
    }

    public static List<String> getAllFiles(String folderName) {
        File[] files = new File(folderName).listFiles();
        if (files == null) return new ArrayList<>();
        return Arrays.stream(files).map(File::getName).collect(Collectors.toList());
    }

    public static void delete(String folder, String filename) throws IOException {
        String filePath = folder + "/" + filename;
        Files.delete(Paths.get(filePath));
    }
}
