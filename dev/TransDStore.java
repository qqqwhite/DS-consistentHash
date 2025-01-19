import java.util.*;
import java.util.stream.Collectors;

/**
 * rebalance 命令生成类，统一命令生成过程，简化操作，提高代码复用率
 */
public class TransDStore {
    private int sourceDPort; // rebalance 命令将要发送到的dstore
    public Map<String, List<Integer>> sendFiles; // sourceDPort send file to sendFiles.KeySet()
    public List<String> removeFiles; //

    TransDStore(int dPort){
        this.sourceDPort = dPort;
        sendFiles = new HashMap<>();
        removeFiles = new ArrayList<>();
    }

    public String generateCommand(){
        List<String> sendFileCommands = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry:sendFiles.entrySet()){
            sendFileCommands.add(String.join(" ", String.format("%s %d",entry.getKey(), entry.getValue().size()),
                    entry.getValue().stream().map(Objects::toString).collect(Collectors.joining(" "))));
        }
        String sendFileCommand = String.join(" ", sendFileCommands);
        String removeFileCommand = String.join(" ", removeFiles);
        String command = String.format("%d %s %d %s", sendFiles.size(), sendFileCommand, removeFiles.size(), removeFileCommand);
        return command;
    }

}
