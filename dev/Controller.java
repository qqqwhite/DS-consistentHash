import java.io.*;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Controller {
    private static final Logger logger = Logger.getLogger(Controller.class.getName());
    private ConcurrentHashMap<Integer, List<String>> dPortsFileMap = new ConcurrentHashMap<>();
    private ServerSocket service;
    private int cPort;
    private int r;
    private int timeout;
    private int rebalancedPeriod;
    private ConcurrentHashMap<String, ControllerFileWrapper> fileMap = new ConcurrentHashMap<>(); // only contains stored dStore port, including the lost files
    private ConsistentHash consistentHash = new ConsistentHash();


    public static void main(String[] args) throws IOException {
        if (args.length == 4) {
            int cPort = Integer.parseInt(args[0]);
            int r = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalancedPeriod = Integer.parseInt(args[3]);
            Controller controller = new Controller(cPort, r, timeout, rebalancedPeriod);
        } else {
            // test module
            logger.info("test module");
            Controller controller = new Controller(8000, 3, 2000, 20000);
        }
    }

    public Controller(int cPort, int r, int timeout, int rebalancedPeriod) throws IOException {
        this.cPort = cPort;
        this.r = r;
        this.timeout = timeout;
        this.rebalancedPeriod = rebalancedPeriod;
        if (!Config.skipRebalanced) {
            Timer timer = new Timer();
            // 定时执行rebalanced
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        int count = 0;
                        while (!rebalanced(null)) {
                            count++;
                            if (count > 5){
                                throw new RuntimeException();
                            }
                        }
                        ;
                    } catch (ConsistentHashException e) {
                        logger.warning("rebalanced fault");
                    }
                }
            };
            timer.schedule(timerTask, this.rebalancedPeriod, this.rebalancedPeriod);
        }
        service = new ServerSocket(this.cPort);
        while (true) {
            logger.info(String.format("listening on: %d", this.cPort));
            Communication tmpService = new Communication(service.accept(), this.cPort);
            logger.info(String.format("established on %d with %d", this.cPort, tmpService.getTargetPort()));
            // 每一个线程用于处理一个socket通信
            new Thread() {
                {
                    setName(String.format("controller %d %d", cPort, tmpService.getTargetPort()));
                }
                private Map<String, List<Integer>> errorPortsMap = new HashMap<>();
                private Map<String, Integer> lastLoadPort = new HashMap<>();

                @Override
                public void run() {
                    try {
                        int tolerance = 0;
                        while (!tmpService.isClosed()) {
                            if (tolerance > 10){
                                throw new SocketTimeoutException("out of tolerance");
                            }
                            String msg = null;
                            try {
                                msg = tmpService.receive();
                            } catch (SocketTimeoutException e){
                                logger.info("tolerance timeout error");
                                tolerance++;
                                continue;
                            }
                            if (msg != null && !msg.isEmpty()) {
                                logger.info(String.format("handle msg %s on %s", msg, Thread.currentThread().getName()));
                                long startTime = System.currentTimeMillis();
                                handle(msg, tmpService, errorPortsMap, lastLoadPort);
                                System.out.printf("msg %s cost time %d\n", msg, System.currentTimeMillis() - startTime);
                            }
                            if (msg == null) tmpService.close(); // receive EOF
                        }
                        logger.info(String.format("connection on %s is closed", Thread.currentThread().getName()));
                    } catch (IOException e) {
                        logger.warning("controller socket sync io error");
                    } catch (ConsistentHashException e) {
                        logger.warning("consistent hash executing fault");
                    }
                }
            }.start();
        }
    }

    private Map<Integer, List<String>> rebalancedDStore(){
        return null;
    }

    /**
     * 重平衡
     * @param newDPort
     * @return
     * @throws ConsistentHashException
     */
    private synchronized boolean rebalanced(Integer newDPort) throws ConsistentHashException {
        long startTime = System.currentTimeMillis();
        logger.info("==================== start rebalanced ====================");
        ExecutorService executorService = Executors.newFixedThreadPool(dPortsFileMap.size());
        Map<Integer, List<String>> dPortStoredFilesMap = new ConcurrentHashMap<>();
        Map<Integer, List<String>> dPortTransFilesMap = new HashMap<>(); // Map<target dPort, filenames to be moved to new dPort>
        Map<Integer, List<String>> dPortRemoveFilesMap = new ConcurrentHashMap<>(); // Map<target dPort, files to be removed>
        Map<String, List<Integer>> storedFileMap = new ConcurrentHashMap<>();
        for (String filename:fileMap.keySet()){
            storedFileMap.put(filename, new ArrayList<>());
        }
        List<Integer> errorDPorts = new ArrayList<>();
        // 获取所有dstore持有的文件列表，并记录有问题的节点
        for (int dPort: dPortsFileMap.keySet()){
            dPortTransFilesMap.put(dPort, new ArrayList<>()); // init
            dPortRemoveFilesMap.put(dPort, new ArrayList<>()); //init
            executorService.submit(() -> {
                try {
                    Communication communication = new Communication(dPort, timeout);
                    communication.send(Protocol.LIST_TOKEN);
                    String receivedMsg = communication.receive();
                    String[] substrings = receivedMsg.split(" ");
                    String protocol = substrings[0];
                    if (protocol.equals(Protocol.LIST_TOKEN)){
                        List<String> storedFiles = new ArrayList<>();
                        for (int i = 1; i<substrings.length; i++){
                            String filename = substrings[i];
                            storedFileMap.get(filename).add(dPort);
                            storedFiles.add(filename);
                        }
                        dPortStoredFilesMap.put(dPort, storedFiles);
                    }else {
                        throw new SocketTimeoutException("unexpected response");
                    }
                    communication.close();
                } catch (IOException e) {
                    logger.info(String.format("dPort %d is breakdown", dPort));
                    synchronized (errorDPorts){
                        errorDPorts.add(dPort);
                    }
                }
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (int errorDPort: errorDPorts){
            List<String> filenames = dPortsFileMap.get((Integer) errorDPort);
            for (String filename:filenames){
                fileMap.get(filename).Dstore.remove((Integer) errorDPort);
            }
            dPortsFileMap.remove((Integer) errorDPort);
        }
            // deal with storage files lost and excluded
        Map<Integer, List<String>> dPortLostFiles = new HashMap<>();
        // 获取存储节点中缺少或多余的文件
        for (Map.Entry<Integer, List<String>> entry: dPortsFileMap.entrySet()){
            List<String> filenameIntersection = new ArrayList<>(entry.getValue());
            filenameIntersection.retainAll(dPortStoredFilesMap.getOrDefault(entry.getKey(), new ArrayList<>()));
            List<String> shouldStoredFilenames = new ArrayList<>(entry.getValue());
            shouldStoredFilenames.removeAll(filenameIntersection); // delta -> lost files
            List<String> realStoredFilenames = new ArrayList<>(dPortStoredFilesMap.getOrDefault(entry.getKey(), new ArrayList<>()));
            realStoredFilenames.removeAll(filenameIntersection);
            dPortTransFilesMap.get(entry.getKey()).addAll(shouldStoredFilenames); // fix lost files
            dPortLostFiles.put(entry.getKey(), shouldStoredFilenames);
            dPortRemoveFilesMap.get(entry.getKey()).addAll(realStoredFilenames);
        }
        // to remove lost file from list, update fileMap
        for (Map.Entry<Integer, List<String>> entry:dPortTransFilesMap.entrySet()){
            for (String filename:entry.getValue()){
                fileMap.get(filename).Dstore.remove(entry.getKey());
            }
        }
        List<Map<Integer, List<VirtualDstore>>> removeDPortTransVDsMaps = new ArrayList<>(); // Map<target dPort, VDs waiting to moved to new dPort>
        for (int errorDPort: errorDPorts){
            Map<Integer, List<VirtualDstore>> removeDPortTransVDsMap = new HashMap<>();
            removeDPortTransVDsMap = consistentHash.removeDstore(errorDPort);
            removeDPortTransVDsMaps.add(removeDPortTransVDsMap);
            dPortTransFilesMap.remove((Integer) errorDPort);
            dPortRemoveFilesMap.remove((Integer) errorDPort);
        }
        // make sure that remove dstore and fix lost file is handle before add dstore
        // got dPortLostFiles & errorDPorts
//        List<VirtualDstore> moveToNewDStore = new ArrayList<>(); // TODO: in theory we don't need moveTONewDStore
        Map<Integer, List<VirtualDstore>> addDPortTransVDMap = new HashMap<>();
        if (newDPort != null) {
            addDPortTransVDMap = consistentHash.addDstore(newDPort); // Map<dPort contained VD, VD need to be trans to new dPort>
//            for (Map.Entry<Integer, List<VirtualDstore>> entry : addDPortTransVDMap.entrySet()) {
//                for (VirtualDstore addVD : entry.getValue()){
//                    if (removeVDsDPortMap.containsKey(addVD)){
//                        for (Map<Integer, List<VirtualDstore>> removeDPortTransVDsMap: removeDPortTransVDsMaps){
//                            moveToNewDStore.add(addVD);
//                            removeDPortTransVDsMap.get(removeVDsDPortMap.get(addVD)).remove(addVD);
//                        }
//                    }
//                }
//            }
            for (int errorDPort: errorDPorts){
                assert !addDPortTransVDMap.containsKey(errorDPort);
                addDPortTransVDMap.remove(errorDPort);
            }
        }
        for (Map<Integer, List<VirtualDstore>> dPortTransVDsMap:removeDPortTransVDsMaps){
            for (Map.Entry<Integer, List<VirtualDstore>> entry: dPortTransVDsMap.entrySet()){
                List<String> filenames = new ArrayList<>();
                for (VirtualDstore virtualDstore:entry.getValue()){
                    if (!virtualDstore.files.isEmpty()){
                        filenames.addAll(virtualDstore.files);
                    }
                }
                dPortTransFilesMap.get(entry.getKey()).addAll(filenames);
            }
        }
        // got dPortTransFilesMap & dPortRemoveFilesMap
        // it's time to trans files
        // notice info update and sync
        Map<Integer, TransDStore> transDStores = new HashMap<>();
        for (int dPort: dPortStoredFilesMap.keySet()){
            TransDStore transDStore = new TransDStore(dPort);
            transDStores.put(dPort, transDStore);
        }
        for (Map.Entry<Integer, List<String>> entry:dPortTransFilesMap.entrySet()){
            for (String filename:entry.getValue()){
                int containedDPort = storedFileMap.get(filename).getFirst();
                List<Integer> targetDPorts = transDStores.get(containedDPort).sendFiles.getOrDefault(filename, new ArrayList<>());
                targetDPorts.add(entry.getKey());
                transDStores.get(containedDPort).sendFiles.put(filename, targetDPorts);
            }
        }
        for (Map.Entry<Integer, List<String>> entry: dPortRemoveFilesMap.entrySet()){
            transDStores.get(entry.getKey()).removeFiles.addAll(entry.getValue());
        }
        if (newDPort!=null) {
//            for (VirtualDstore virtualDstore : moveToNewDStore) {
//                for (String filename : virtualDstore.files) {
//                    List<Integer> dPorts = fileMap.get(filename).Dstore;
//                    dPorts.removeAll(errorDPorts);
//                    List<Integer> targetDPorts = transDStores.get(dPorts.getFirst()).sendFiles.getOrDefault(filename, new ArrayList<>());
//                    targetDPorts.add(newDPort);
//                    transDStores.get(dPorts.getFirst()).sendFiles.put(filename, targetDPorts);
//                }
//            }
            // 处理新增节点的文件转移
            for (Map.Entry<Integer, List<VirtualDstore>> entry: addDPortTransVDMap.entrySet()){
                // trans addDPortTransVDMap to transDStore
                for (VirtualDstore virtualDstore:entry.getValue()){
                    if (virtualDstore.files.isEmpty()) continue;
                    TransDStore target = transDStores.get(entry.getKey());
                    for (String filename: virtualDstore.files){
                        List<Integer> targetDPorts = target.sendFiles.getOrDefault(filename, new ArrayList<>());
                        targetDPorts.add(newDPort);
                        target.sendFiles.put(filename, targetDPorts);
                    }
                    target.removeFiles.addAll(virtualDstore.files);
                }
            }
        }
        executorService = Executors.newFixedThreadPool(transDStores.size());
        List<Integer> rebalancedDPorts = new ArrayList<>();
        List<Integer> faultRebalancedDPorts = new ArrayList<>();
        // 遍历当前可运行节点，完成文件转移和删除操作
        for (Map.Entry<Integer, TransDStore> entry: transDStores.entrySet()){
            if (entry.getValue().removeFiles.isEmpty() && entry.getValue().sendFiles.isEmpty()){
                continue;
            }
            executorService.submit(() -> {
                try {
                    Communication communication = new Communication(entry.getKey(), timeout);
                    String sendMsg = String.join(" ", Protocol.REBALANCE_TOKEN, entry.getValue().generateCommand());
                    logger.info(String.format("rebalanced %d with %s", entry.getKey(), sendMsg));
                    communication.send(sendMsg);
                    String receivedMsg = communication.receive();
                    String[] substrings = receivedMsg.split(" ");
                    String protocol = substrings[0];
                    if (protocol.equals(Protocol.REBALANCE_COMPLETE_TOKEN)){
                        rebalancedDPorts.add(entry.getKey());
                        dPortsFileMap.get(entry.getKey()).removeAll(entry.getValue().removeFiles);
                        for (String removeFile:entry.getValue().removeFiles){
                            ControllerFileWrapper controllerFileWrapper = fileMap.get(removeFile);
                            controllerFileWrapper.Dstore.remove((Integer) entry.getKey());
                        }
                        for (Map.Entry<String, List<Integer>> entry1:entry.getValue().sendFiles.entrySet()){
                            for (int dPort:entry1.getValue()){
                                dPortsFileMap.get(dPort).add(entry1.getKey());
                            }
                            ControllerFileWrapper controllerFileWrapper = fileMap.get(entry1.getKey());
                            controllerFileWrapper.Dstore.addAll(entry1.getValue());
                        }
                    }else {
                        throw new SocketTimeoutException("unexpected response");
                    }
                    communication.close();
                } catch (SocketTimeoutException e) {
                    faultRebalancedDPorts.add(entry.getKey());
                    logger.warning(String.format("%d rebalanced fault", entry.getKey()));
                } catch (IOException e) {
                    logger.warning("socket io error");
                }
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //TODO after
        for (Map<Integer, List<VirtualDstore>> map:removeDPortTransVDsMaps){
            consistentHash.afterRemoveDstore(map);
        }
        if (newDPort!=null) {
            // TODO: store file changed info into consistentHash.fileVDstoreMap
            consistentHash.afterAddDstore(newDPort, addDPortTransVDMap);
        }
        boolean isSuccess = faultRebalancedDPorts.isEmpty();
        if (isSuccess) {
            logger.info("==================== end rebalanced successfully ====================");
        }else {
            logger.info("==================== end rebalanced failed ====================");
        }
        System.out.printf("rebalanced cost %d\n", System.currentTimeMillis() - startTime);
        return isSuccess;
    }

    /**
     * 命令处理
     * @param msg
     * @param tmpService
     * @param errorPortsMap
     * @param lastLoadPort
     * @throws IOException
     * @throws ConsistentHashException
     */
    private void handle(String msg, Communication tmpService,
                        Map<String, List<Integer>> errorPortsMap,
                        Map<String, Integer> lastLoadPort
                        ) throws IOException, ConsistentHashException {
        logger.info(String.format("message: %s", msg));
        String[] substrings = msg.split(" ");
        String protocol = substrings[0];
        switch (protocol) {
            case Protocol.JOIN_TOKEN -> { // join
                int dPort = Integer.parseInt(substrings[1]);
                Communication tmpClient = new Communication(dPort, this.timeout);
                tmpClient.send(Protocol.JOIN_TOKEN);
                tmpClient.close();
                if (dPortsFileMap.size() == r - 1) {
                    List<Integer> dPorts = new ArrayList<>(dPortsFileMap.keySet());
                    dPorts.add(dPort);
                    consistentHash.initConsistentHash(dPorts);
                } else if (dPortsFileMap.size() >= r) {
                    rebalanced(dPort);
                }
                dPortsFileMap.put(dPort, new ArrayList<>());
            }
            case Protocol.LIST_TOKEN -> { // list
                if (dPortsFileMap.size() < r) {
                    tmpService.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    return;
                }
                String files = String.join(" ", getFileByStatus(DstoreFileStatus.STORE_COMPLETE));
                logger.info(String.format("stored files are %s", files));
                String sendMsg = String.join(" ", new String[]{Protocol.LIST_TOKEN, files});
                tmpService.send(sendMsg);
            }
            case Protocol.STORE_TOKEN -> { // store
                if (dPortsFileMap.size() < r) {
                    tmpService.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    return;
                }
                String filename = substrings[1];
                int filesize = Integer.parseInt((substrings[2]));
                if (fileMap.containsKey(filename) && fileMap.get(filename).status != DstoreFileStatus.REMOVE_COMPLETE) {
                    logger.warning(String.format("file %s already exist", filename));
                    tmpService.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return;
                }
                if (filesize > 100000) {
                    logger.warning("filesize is oversize");
                    tmpService.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return;
                }
                List<VirtualDstore> targetVDs = consistentHash.getStoreTarget(filename, r);
//            List<Integer> dstore = getStorePorts(filename, filesize);
                List<Integer> dstore = new ArrayList<>();
                for (VirtualDstore virtualDstore : targetVDs) {
                    dstore.add(virtualDstore.dPort);
                }
                ControllerFileWrapper controllerFileWrapper = new ControllerFileWrapper(filename, filesize, new ArrayList<>(), DstoreFileStatus.STORE_IN_PROGRESS);
                fileMap.put(filename, controllerFileWrapper);
                String dstorePorts = String.join(" ", dstore.stream().map(Object::toString).toArray(String[]::new));
                String sendMsg = String.join(" ", new String[]{Protocol.STORE_TO_TOKEN, dstorePorts});
                tmpService.send(sendMsg);
                // waiting for dstore msg
                try {
                    ControllerFileWrapper tmpCFW = fileMap.get(filename);
                    synchronized (tmpCFW) {
                        tmpCFW.wait(timeout);
                        if (tmpCFW.status != DstoreFileStatus.STORE_COMPLETE){
                            fileMap.remove(filename);
//                            tmpService.close();
                            logger.warning(String.format("store file %s error", filename));
                            return;
                        }
                    }
                    consistentHash.afterStoreTargetFile(filename, targetVDs);
                    tmpService.send(Protocol.STORE_COMPLETE_TOKEN);
                } catch (InterruptedException e) {
                    logger.warning("thread waiting error");
                }
            }
            case Protocol.STORE_ACK_TOKEN -> { // dstore ack store
                String filename = substrings[1];
                int dPort = Integer.parseInt(substrings[2]);
                dPortsFileMap.get(dPort).add(filename);
                ControllerFileWrapper tmpCFW = fileMap.get(filename);
                synchronized (tmpCFW) {
                    List<Integer> storedPorts = tmpCFW.Dstore;
                    storedPorts.add(dPort);
                    if (storedPorts.size() == r) {
                        tmpCFW.status = DstoreFileStatus.STORE_COMPLETE;
                        tmpCFW.notifyAll();
                    }
                }
            }
            case Protocol.LOAD_TOKEN -> { // load file
                String filename = substrings[1];
                if (dPortsFileMap.size() < r) {
                    tmpService.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    return;
                }
                ControllerFileWrapper tmpCFW = fileMap.getOrDefault(filename, null);
                if (tmpCFW == null || tmpCFW.status != DstoreFileStatus.STORE_COMPLETE) {
                    tmpService.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return;
                }
                int filesize = tmpCFW.fileSize;
//                if (!errorPortsMap.containsKey(filename)) {
//                    errorPortsMap.put(filename, new ArrayList<>());
//                }
//                int selectedPort = selectLoadPort(tmpCFW, errorPortsMap.get(filename));
                int selectedPort = tmpCFW.Dstore.getFirst();
                errorPortsMap.remove(filename);
                lastLoadPort.put(filename, selectedPort);
                assert selectedPort != -1;
                logger.info(String.format("selected %d port to send file %s", selectedPort, filename));
                String sendMsg = String.join(" ", new String[]{Protocol.LOAD_FROM_TOKEN, String.valueOf(selectedPort), String.valueOf(filesize)});
                tmpService.send(sendMsg);
            }
            case Protocol.RELOAD_TOKEN -> {
                String filename = substrings[1];
                if (!errorPortsMap.containsKey(filename)){
                    List<Integer> tmp = new ArrayList<>();
                    tmp.add(lastLoadPort.get(filename));
                    errorPortsMap.put(filename, tmp);
                }
                ControllerFileWrapper tmpCFW = fileMap.getOrDefault(filename, null);
                assert tmpCFW != null;
                int filesize = tmpCFW.fileSize;
                int selectedPort = selectLoadPort(tmpCFW, errorPortsMap.get(filename));
                if (selectedPort == -1) {
                    logger.warning(String.format("all distribute stores are unavailable for file %s", filename));
                    tmpService.send(Protocol.ERROR_LOAD_TOKEN);
                    return;
                }
                logger.info(String.format("RELOAD: selected %d port to send file %s", selectedPort, filename));
                String sendMsg = String.join(" ", new String[]{Protocol.LOAD_FROM_TOKEN, String.valueOf(selectedPort), String.valueOf(filesize)});
                tmpService.send(sendMsg);
            }
            case Protocol.REMOVE_TOKEN -> {
                if (dPortsFileMap.size() < r) {
                    tmpService.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    return;
                }
                String filename = substrings[1];
                ControllerFileWrapper tmpCFW = fileMap.getOrDefault(filename, null);
                if (tmpCFW == null || tmpCFW.status != DstoreFileStatus.STORE_COMPLETE) {
                    tmpService.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return;
                }
                tmpCFW.status = DstoreFileStatus.REMOVE_IN_PROGRESS;
                List<Thread> threads = new ArrayList<>();
                for (int dPort : tmpCFW.Dstore) {
                    Thread thread = new Thread() {
                        {
                            setName(String.format("controller %d %d", cPort, dPort));
                        }

                        @Override
                        public void run() {
                            try {
                                Communication tmpClient = new Communication(dPort, timeout);
                                String sendMsg = String.join(" ", new String[]{Protocol.REMOVE_TOKEN, filename});
                                tmpClient.send(sendMsg);
                                String receiveMsg = tmpClient.receive();
                                if ((receiveMsg.contains(Protocol.REMOVE_ACK_TOKEN)
                                        || receiveMsg.contains(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN))
                                        && receiveMsg.contains(filename)) {
                                    dPortsFileMap.get(Integer.valueOf(dPort)).remove(filename);
                                    synchronized (tmpCFW) {
                                        tmpCFW.Dstore.remove(Integer.valueOf(dPort)); // it's the reason why don't start threads in the loop
                                        if (tmpCFW.Dstore.isEmpty()) {
                                            tmpCFW.status = DstoreFileStatus.REMOVE_COMPLETE;
                                            tmpCFW.notifyAll();
                                        }
                                    }
                                } else {
                                    throw new RuntimeException("not the expected response");
                                }
                            } catch (SocketTimeoutException e) {
                                logger.warning(String.format("not receive response of REMOVE command from %d", dPort));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                    threads.add(thread);
                }
                Thread tmpServiceThread = new Thread() {
                    {
                        setName(String.format("controller %d %d", cPort, tmpService.getTargetPort()));
                    }

                    @Override
                    public void run() {
                        try {
                            synchronized (tmpCFW) {
                                while (tmpCFW.status != DstoreFileStatus.REMOVE_COMPLETE) {
                                    tmpCFW.wait();
                                    logger.info(String.format("Thread %s has been notified", Thread.currentThread().getName()));
                                }
                            }
                            tmpService.send(Protocol.REMOVE_COMPLETE_TOKEN);
                            logger.info(String.format("successfully remove file %s", filename));
                        } catch (InterruptedException e) {
                            logger.warning("thread waiting error");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                tmpServiceThread.start();
                for (Thread thread : threads) thread.start();
                try {
                    tmpServiceThread.join(timeout);
                    consistentHash.removeFile(filename);
                    logger.info(String.format("successfully remove file %s", filename));
                } catch (InterruptedException e) { // don't receive enough ACKs
                    logger.warning(String.format("REMOVE command is timeout, fail dstore ports are %s", tmpCFW.Dstore.stream().map(Object::toString).collect(Collectors.joining(" "))));
                }
            }
        }
    }

    private String sendAndReceiveToDstore(int dPort, String sendMsg) throws IOException {
        Communication tmpClient = new Communication(dPort, this.timeout);
        tmpClient.send(sendMsg);
        String receiveMsg = tmpClient.receive(); // handle SocketTimeoutException outside on diff condition
        tmpClient.close();
        tmpClient = null; // GC instance
        return receiveMsg;
    }

    private List<String> getFileByStatus(DstoreFileStatus status) {
        List<String> fileNames = new ArrayList<>();
        for (ControllerFileWrapper controllerFileWrapper : fileMap.values()) {
            if (controllerFileWrapper.status == status) {
                fileNames.add(controllerFileWrapper.fileName);
            }
        }
        return fileNames;
    }

    @Deprecated
    private List<Integer> getStorePorts(String filename, int filesize) throws ConsistentHashException {
        int beginIndex = 0;
        List<Integer> dPorts = new ArrayList<>(dPortsFileMap.keySet());
        for (int i = 1; i < dPorts.size(); i++) {
            if (dPortsFileMap.get(dPorts.get(i)).size() < dPortsFileMap.get(dPorts.get(i - 1)).size()) {
                beginIndex = i;
                break;
            }
        }
        List<Integer> selectedDPorts = new ArrayList<>();
        for (int i = 0; i < r; i++) {
            int dPort = dPorts.get((beginIndex + i) % dPorts.size());
            selectedDPorts.add(dPort);
            dPortsFileMap.get(dPort).add(filename);
        }
        logger.info(String.format("store file %s to %s", filename, selectedDPorts.stream().map(Object::toString).collect(Collectors.joining(" "))));
        return selectedDPorts;
    }

    private int selectLoadPort(ControllerFileWrapper tmpCFW, List<Integer> errorPort){
        for (int dPort:tmpCFW.Dstore){
            if (!errorPort.contains(dPort)){
                errorPort.add(dPort);
                return dPort;
            }
        }
        return -1;
    }
}