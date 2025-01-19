
import java.io.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.logging.Logger;

public class Dstore {
    private static final Logger logger = Logger.getLogger(Dstore.class.getName());
    private ServerSocket service;
    private int dPort;
    private int cPort;
    private int timeout;
    private String fileFolder;
    private Communication controllerCommunication;

    public static void main(String[] args) throws IOException {
        if (args.length == 4) {
            int dPort = Integer.parseInt(args[0]);
            int cPort = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            String fileFolder = args[3];
            Dstore dstore = new Dstore(dPort, cPort, timeout, fileFolder);
        } else {
            // test module
            logger.info("test module");
            Dstore dstore = new Dstore(8001, 8000, 2000, "file_folder_8001");
        }
    }

    /**
     * 初始化
     * @param dPort
     * @param cPort
     * @param timeout
     * @param fileFolder
     * @throws IOException
     */
    public Dstore(int dPort, int cPort, int timeout, String fileFolder) throws IOException {
        this.dPort = dPort;
        this.cPort = cPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        FileUtils.createFolder(this.fileFolder);
        service = new ServerSocket(dPort);
        controllerCommunication = new Communication(this.cPort);
        String sendMsg = String.join(" ", Protocol.JOIN_TOKEN, String.valueOf(dPort));
        controllerCommunication.send(sendMsg);
//        tmpClient.close();
//        tmpClient = null; // GC instance
        while (true) {
            logger.info(String.format("listening on: %d", dPort));
            Communication tmpService = new Communication(service.accept(), this.dPort, this.timeout);
            logger.info(String.format("established on: %d", dPort));
            // 每一个线程用于处理一个socket通信
            new Thread(){
                {
                    setName(String.format("dstore %d %d", dPort, tmpService.getTargetPort()));
                }

                @Override
                public void run() {
                    try {
                        while (!tmpService.isClosed()) {
                            String msg = tmpService.receive(); //TODO: handle timeout error
                            if (msg != null && !msg.isEmpty()) {
                                handle(msg, tmpService);
                            }
                            if (msg == null) tmpService.close(); // receive EOF
                        }
                    }catch (IOException e){
                        logger.warning(Thread.currentThread().getName()+"dstore breakdown");
                    }
                }
            }.start();
        }
    }

    /**
     * 命令处理
     * @param msg
     * @param tmpService
     * @throws IOException
     */
    private void handle(String msg, Communication tmpService) throws IOException {
        logger.info(String.format("message: %s", msg));
        String[] substrings = msg.split(" ");
        String protocol = substrings[0];
        switch (protocol) {
            case Protocol.JOIN_TOKEN -> logger.info(String.format("successful join to controller on %d", dPort));
            case Protocol.STORE_TOKEN -> {
                String filename = substrings[1];
                int filesize = Integer.parseInt((substrings[2]));
                tmpService.send(Protocol.ACK_TOKEN);
                byte[] fileContent = tmpService.receiveContent(filesize);
                try {
                    FileUtils.storeFile(fileFolder, filename, fileContent);
                    String sendMsg = String.join(" ", new String[]{Protocol.STORE_ACK_TOKEN, filename, String.valueOf(dPort)});
                    controllerCommunication.send(sendMsg);
//                    tmpClient.send(sendMsg);
//                    tmpClient.close();
                } catch (IOException e) {
                    logger.warning(String.format("fail storing file %s", filename));
                }
            }
            case Protocol.LOAD_DATA_TOKEN -> { // load data
                //TODO: might be terminated
                String filename = substrings[1];
                if (!FileUtils.getAllFiles(fileFolder).contains(filename)) {
                    tmpService.close();
                    return;
                }
                byte[] fileContent = FileUtils.loadFile(fileFolder, filename);
                logger.info(String.format("load file %s filesize is %d", filename, fileContent.length));
                tmpService.sendContent(fileContent);
            }
            case Protocol.REMOVE_TOKEN -> {
                //TODO: might be terminated
                String filename = substrings[1];
                if (!FileUtils.getAllFiles(fileFolder).contains(filename)) {
                    String sendMsg = String.join(" ", new String[]{Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, filename});
                    tmpService.send(sendMsg);
                    logger.info(String.format("didn't find the stored file %s", filename));
                    return;
                }
                try {
                    FileUtils.delete(fileFolder, filename);
                    String sendMsg = String.join(" ", new String[]{Protocol.REMOVE_ACK_TOKEN, filename});
                    tmpService.send(sendMsg);
                } catch (IOException e) {
                    logger.warning(String.format("fail to delete file %s", filename));
                }
            }
            case Protocol.LIST_TOKEN -> {
                List<String> files = FileUtils.getAllFiles(fileFolder);
                String sendMsg = String.join(" ", Protocol.LIST_TOKEN, String.join(" ", files));
                tmpService.send(sendMsg);
            }
            case Protocol.REBALANCE_TOKEN -> {
                //TODO: rebalanced
                int pos = 1;
                int sendFileCount = Integer.parseInt(substrings[pos++]);
                boolean isFault = false;
                try {
                    for (int i = 0; i < sendFileCount; i++) {
                        String filename = substrings[pos++];
                        int targetDPortCount = Integer.parseInt(substrings[pos++]);
                        for (int j = 0; j < targetDPortCount; j++) {
                            int targetDPort = Integer.parseInt(substrings[pos++]);
                            Communication tmpClient = new Communication(targetDPort, timeout);
                            byte[] fileContent = FileUtils.loadFile(fileFolder, filename);
                            String sendMsg = String.join(" ", Protocol.REBALANCE_STORE_TOKEN, filename, String.valueOf(fileContent.length));
                            tmpClient.send(sendMsg);
                            String receiveMsg = tmpClient.receive();
                            if (receiveMsg.startsWith(Protocol.ACK_TOKEN)) {
                                tmpClient.sendContent(fileContent);
                            } else {
                                isFault = true;
                            }
                            tmpClient.close();
                        }
                    }
                    int deleteFileCount = Integer.parseInt(substrings[pos++]);
                    for (int i = 0;i<deleteFileCount;i++){
                        String filename = substrings[pos++];
                        FileUtils.delete(fileFolder, filename);
                    }
                    if (!isFault) {
                        tmpService.send(Protocol.REBALANCE_COMPLETE_TOKEN);
                    }
                }catch (IOException e){
                    logger.warning("rebalanced fault");
                }
            } case Protocol.REBALANCE_STORE_TOKEN -> {
                String filename = substrings[1];
                int filesize = Integer.parseInt((substrings[2]));
                tmpService.send(Protocol.ACK_TOKEN);
                byte[] fileContent = tmpService.receiveContent(filesize);
                try {
                    FileUtils.storeFile(fileFolder, filename, fileContent);
                } catch (IOException e) {
                    logger.warning(String.format("fail storing file %s", filename));
                }
            }
        }
    }




}