import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 一致性hash算法实现类
 */
public class ConsistentHash {

    public Map<Integer, VirtualDstore> virtualDstoreMap = new TreeMap<>(); // inflect VD to hash ring
    public Map<Integer, List<VirtualDstore>> realVirtualDstoreMap = new ConcurrentHashMap<>(); // inflect RD to VDs
    public List<Integer> dPorts = new ArrayList<>();
    public Map<String, List<VirtualDstore>> fileVDstoreMap = new ConcurrentHashMap<>(); // inflect filename to VDs
    public List<Integer> hashPosList = new ArrayList<>(); // to accelerate VDMap.KeySet()

    private static final MessageDigest md;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public ConsistentHash(){}

    /**
     * 初始化
     * @param dPorts
     * @throws ConsistentHashException
     */
    public void initConsistentHash(List<Integer> dPorts) throws ConsistentHashException {
        // hash ring is range(0, MAX)
        int virtualDstoreInterval = Integer.MAX_VALUE / Config.virtualDstoreCount;
        int virtualDstoreReminder = Integer.MAX_VALUE % Config.virtualDstoreCount + 1;
        if (virtualDstoreReminder == Config.virtualDstoreCount) {
            virtualDstoreReminder = 0;
            virtualDstoreInterval += 1;
        }
        int VDbase = -1;
        // 分配虚拟节点，将虚拟节点映射到hash环上的某一段上
        for (int i = 0; i < Config.virtualDstoreCount; i++) {
            VDbase += virtualDstoreInterval;
            if (virtualDstoreReminder > i) VDbase += 1;
            virtualDstoreMap.put(VDbase, VirtualDstore.newInstance());
        }
        if (VDbase != Integer.MAX_VALUE) throw new ConsistentHashException("virtual nodes init fault");
        // vDstore id is range(0, count-1)
        int VDstoreGroupBaseCount = Config.virtualDstoreCount / dPorts.size();
        int dstoreReminder = Config.virtualDstoreCount % dPorts.size();
        Iterator<VirtualDstore> virtualDstoreIterator = virtualDstoreMap.values().iterator();
        // 分配实际数据节点，建立虚拟节点到实际存储节点的多到1的映射关系
        for (int dPort : dPorts) {
            List<VirtualDstore> virtualDstore = new ArrayList<>();
            int count = VDstoreGroupBaseCount;
            if (dstoreReminder > 0) {
                dstoreReminder -= 1;
                count++;
            }
            for (int i = 0; i < count; i++) {
                VirtualDstore virtualDstore1 = virtualDstoreIterator.next();
                virtualDstore1.dPort = dPort;
                virtualDstore.add(virtualDstore1);
            }
            realVirtualDstoreMap.put(dPort, virtualDstore);
            this.dPorts.add(dPort);
        }
        hashPosList = new ArrayList<>(virtualDstoreMap.keySet());
    }

    /**
     * 寻找距离hash值最近的虚拟节点，需要高性能
     * @param hash
     * @return
     * @throws ConsistentHashException
     */
    private VirtualDstore searchNearest(int hash) throws ConsistentHashException {
        int inferEndCount = hash/(Integer.MAX_VALUE/Config.virtualDstoreCount + 1);
        while (hashPosList.get(inferEndCount) < hash){
            inferEndCount++;
        }
        return virtualDstoreMap.get(hashPosList.get(inferEndCount));
    }

    /**
     * 获取文件存储的虚拟节点
     * @param filename
     * @param r
     * @return
     * @throws ConsistentHashException
     */
    public List<VirtualDstore> getStoreTarget(String filename, int r) throws ConsistentHashException {
        int hashCode = Utils.getHash(filename, md);
        List<VirtualDstore> virtualDstoreList = new ArrayList<>();
        List<Integer> selectedDPorts = new ArrayList<>();
        // 根据文件名的hash值，获取要存储的虚拟节点，并避免文件的副本落在同一个实际存储节点上
        for (int i = 0; i<r; i++) {
            VirtualDstore selected = searchNearest(hashCode);
            int reHashCount = 0;
            while (selectedDPorts.contains(selected.dPort)) {
//                hashCode = hashCode - Integer.MAX_VALUE + Integer.MAX_VALUE / realVirtualDstoreMap.size();
//                if (hashCode < 0) hashCode += Integer.MAX_VALUE;
                hashCode = Utils.getHash(String.valueOf(hashCode), md);
                reHashCount++;
                selected = searchNearest(hashCode);
            }
            //TODO: balance
            hashCode = Integer.hashCode(hashCode);
            selectedDPorts.add(selected.dPort);
            virtualDstoreList.add(selected);
            if (reHashCount>10) {
                // 监控算法性能，如果rehash次数过多，说明重新挑选了太多次虚拟节点，即实际存储节点的碰撞概率太高，文件副本容易落在同一个实际存储节点上，
                // 检查hash算法是否足够散列，虚拟节点到实际节点的映射关系是否足够随机
                System.out.printf("rehash times: %d%n", reHashCount);
            }
        }
        // adding file is outside this func when controller has edited the stored file status
        return virtualDstoreList;
    }

    /**
     * 文件存储结束，同步算法内部文件信息
     * @param filename
     * @param virtualDstoreList
     */
    public synchronized void afterStoreTargetFile(String filename, List<VirtualDstore> virtualDstoreList){
        fileVDstoreMap.put(filename, virtualDstoreList);
        for (VirtualDstore virtualDstore: virtualDstoreList){
            virtualDstore.files.add(filename);
        }
    }

    /**
     * 移除文件
     * @param filename
     * @throws ConsistentHashException
     */
    public void removeFile(String filename) throws ConsistentHashException {
        if (!fileVDstoreMap.containsKey(filename)){
            throw new ConsistentHashException("file-VDstore reflecting fault");
        }
        List<VirtualDstore> virtualDstoreList = fileVDstoreMap.get(filename);
        for (VirtualDstore virtualDstore:virtualDstoreList){
            virtualDstore.files.remove(filename);
        }
        fileVDstoreMap.remove(filename);
    }

    /**
     * 动态添加实际存储节点
     * @param newDPort
     * @return
     * @throws ConsistentHashException
     */
    public Map<Integer, List<VirtualDstore>> addDstore(int newDPort) throws ConsistentHashException {
        int oldVDGroupBaseCount = Config.virtualDstoreCount / dPorts.size();
        int oldVDGroupReminder = Config.virtualDstoreCount % dPorts.size();
        int VDGroupBaseCount = Config.virtualDstoreCount / (dPorts.size() + 1);
        int VDGroupReminder = Config.virtualDstoreCount % (dPorts.size() + 1);
        int deltaReminder = oldVDGroupReminder - VDGroupReminder;
        Map<Integer, List<VirtualDstore>> transferMap = new HashMap<>();
        List<String> transferFilenames = new ArrayList<>();
        // 遍历所有实际存储节点，从节点中挑选适合的虚拟节点，转移给新增节点，并对虚拟节点中对应文件进行转移
        for (Map.Entry<Integer, List<VirtualDstore>> item:realVirtualDstoreMap.entrySet()){
            List<VirtualDstore> tmpTransferVDList = new ArrayList<>();
            int count = oldVDGroupBaseCount - VDGroupBaseCount;
            List<VirtualDstore> tmpOriginVDList = item.getValue();
            if (deltaReminder > 0 && tmpOriginVDList.size()>oldVDGroupBaseCount){
                count++;
                deltaReminder--;
            }else if (deltaReminder < 0 && tmpOriginVDList.size()==oldVDGroupReminder){
                count--;
                deltaReminder++;
            }
            if (count > tmpOriginVDList.size()) throw new ConsistentHashException("transfer virtual dstore calculating fault");
            int rePick = 0;
            int beginPos = 0;
            while (tmpTransferVDList.size()<count){
                VirtualDstore transferVD = tmpOriginVDList.get(beginPos++);
                // 避免新增存储节点含有相同的文件副本
                if (!Utils.containsAny(transferFilenames, transferVD.files)) {
                    transferFilenames.addAll(transferVD.files);
                    tmpTransferVDList.add(transferVD);
                }else {
                    rePick++;
                }
            }
            // 同上，算法性能监控
            System.out.printf("rePick is %d\n", rePick);
            transferMap.put(item.getKey(), tmpTransferVDList);
        }
        return transferMap;
    }

    /**
     * 动态新增存储节点后置处理，进行算法信息同步
     * @param dPort
     * @param transferMap
     */
    public void afterAddDstore(int dPort, Map<Integer, List<VirtualDstore>> transferMap){
        List<VirtualDstore> addVDs = new ArrayList<>();
        dPorts.add(dPort);
        for (Map.Entry<Integer, List<VirtualDstore>> entry: transferMap.entrySet()){
            realVirtualDstoreMap.get(entry.getKey()).removeAll(entry.getValue());
            addVDs.addAll(entry.getValue());
        }
        for (VirtualDstore virtualDstore:addVDs){
            virtualDstore.dPort = dPort;
        }
        realVirtualDstoreMap.put(dPort, addVDs);
    }

    /**
     * 动态移除问题存储节点
     * @param dPort
     * @return
     * @throws ConsistentHashException
     */
    public Map<Integer, List<VirtualDstore>> removeDstore(int dPort) throws ConsistentHashException {
        if (!realVirtualDstoreMap.containsKey(dPort)) throw new ConsistentHashException("unexpected dPort");
        int oldVDGroupBaseCount = Config.virtualDstoreCount / realVirtualDstoreMap.size();
        int oldVDGroupReminder = Config.virtualDstoreCount % realVirtualDstoreMap.size();
        int VDGroupBaseCount = Config.virtualDstoreCount / (realVirtualDstoreMap.size() - 1);
        int VDGroupReminder = Config.virtualDstoreCount % (realVirtualDstoreMap.size() - 1);
        int deltaReminder = oldVDGroupReminder - VDGroupReminder;
        Map<Integer, List<VirtualDstore>> transferMap = new HashMap<>();
        List<VirtualDstore> waitingList = realVirtualDstoreMap.get(dPort);
        realVirtualDstoreMap.remove(dPort);
        dPorts.remove((Integer) dPort);
        List<Integer> dPortsList = new ArrayList<>(realVirtualDstoreMap.keySet());
        Map<Integer, Integer> dPortCapacityMap = new TreeMap<>();
        int dPortPos = 0;
        // 遍历当前正常存储节点，获取每个节点待分配虚拟节点的个数
        while (dPortPos < dPortsList.size()){
            int tmpDPort = dPortsList.get(dPortPos++);
            List<VirtualDstore> tmpOriginVDList = realVirtualDstoreMap.get(tmpDPort);
            int count = oldVDGroupBaseCount - VDGroupBaseCount;
            if (deltaReminder < 0 && tmpOriginVDList.size() == oldVDGroupBaseCount){
                count--;
                deltaReminder++;
            }else if(deltaReminder > 0 && tmpOriginVDList.size() > oldVDGroupBaseCount){
                count++;
                deltaReminder--;
            }
            if (-count > waitingList.size()) {
                throw new ConsistentHashException("transfer virtual dstore calculating fault");
            }
            dPortCapacityMap.put(tmpDPort, count);
            transferMap.put(tmpDPort, new ArrayList<>());
        }
        dPortPos = 0;
        int rePick = 0;
        // 将问题节点的虚拟节点，分配到正常运行的节点上
        for (int i = 0; i<waitingList.size(); i++){
            VirtualDstore transferVD = waitingList.get(i);
            List<VirtualDstore> backupVDs = new ArrayList<>();
            for (String filename:transferVD.files){
                backupVDs.addAll(fileVDstoreMap.get(filename));
            }
            if (!backupVDs.isEmpty()) {
                // 遇到冲突的虚拟节点，首选不是跳过，而是优先分配，避免死锁
                while (Utils.containsAny(realVirtualDstoreMap.get(dPortsList.get(dPortPos)), backupVDs)) {
                    dPortPos = (dPortPos + 1) % dPortCapacityMap.size();
                }
            }
            int targetDPort = dPortsList.get(dPortPos);
            if (dPortCapacityMap.get(targetDPort) < 0){
                transferMap.get(targetDPort).add(transferVD);
                dPortCapacityMap.put(targetDPort, dPortCapacityMap.get(targetDPort) + 1);
            }else {
                transferMap.get(targetDPort).add(transferVD);
                waitingList.add(transferMap.get(targetDPort).removeFirst());
                rePick++;
            }
            dPortPos = (dPortPos + 1) % dPortCapacityMap.size();
        }
        System.out.printf("rePick is %d\n", rePick);
        return transferMap;
    }

    /**
     * 移除问题节点后置处理
     * @param transferMap
     */
    public void afterRemoveDstore(Map<Integer, List<VirtualDstore>> transferMap){
        for (Map.Entry<Integer, List<VirtualDstore>> entry: transferMap.entrySet()){
            realVirtualDstoreMap.get(entry.getKey()).addAll(entry.getValue());
            for (VirtualDstore virtualDstore:entry.getValue()){
                virtualDstore.dPort = entry.getKey();
            }
        }
    }


    public Map<Integer, List<String>> getDPortFilesMap() {
        Map<Integer, List<String>> map = new HashMap<>();
        for (Map.Entry<Integer, List<VirtualDstore>> entry : realVirtualDstoreMap.entrySet()){
            List<String> files = new ArrayList<>();
            for (VirtualDstore virtualDstore:entry.getValue()){
                if (!virtualDstore.files.isEmpty()) files.addAll(virtualDstore.files);
            }
            map.put(entry.getKey(), files);
        }
        return map;
    }

    public List<String> getDPortFiles(int dPort){
        List<String> files = new ArrayList<>();
        for (VirtualDstore virtualDstore:realVirtualDstoreMap.get(dPort)){
            if (!virtualDstore.files.isEmpty()) files.addAll(virtualDstore.files);
        }
        return files;
    }

    /**
     * 一致性hash测试代码
     * @param args
     * @throws ConsistentHashException
     */
    public static void main(String[] args) throws ConsistentHashException {
        List<Integer> dPorts = new ArrayList<>();
        dPorts.add(8001);
        dPorts.add(8002);
        dPorts.add(8003);
        dPorts.add(8004);
        dPorts.add(8005);
        dPorts.add(8006);
        ConsistentHash consistentHash = new ConsistentHash();
        consistentHash.initConsistentHash(dPorts);
        for (int i = 0;i<10;i++){
            store(String.format("file%d", i), consistentHash);
        }
        Map<Integer, List<String>> dPortFilesMap = consistentHash.getDPortFilesMap();

        Map<Integer, List<VirtualDstore>> addDstore = consistentHash.addDstore(9000);
        for (Map.Entry<Integer, List<VirtualDstore>> entry:addDstore.entrySet()){
            List<String> files = new ArrayList<>();
            for (VirtualDstore virtualDstore:entry.getValue()){
                if (!virtualDstore.files.isEmpty()) files.addAll(virtualDstore.files);
            }
            System.out.printf("%d transfer files count is %s\n", entry.getKey(), files.size());
            System.out.println(files);
        }
        Map<Integer, List<VirtualDstore>> removeDstore = consistentHash.removeDstore(8001);
        for (Map.Entry<Integer, List<VirtualDstore>> entry:removeDstore.entrySet()){
            List<String> files = new ArrayList<>();
            for (VirtualDstore virtualDstore:entry.getValue()){
                if (!virtualDstore.files.isEmpty()) files.addAll(virtualDstore.files);
            }
            System.out.printf("%d transfer files count is %s\n", entry.getKey(), files.size());
            System.out.println(files);
        }
        int a = 0;

    }

    public static void store(String filename, ConsistentHash consistentHash) throws ConsistentHashException {
        List<VirtualDstore> list = consistentHash.getStoreTarget(filename, 2);
        consistentHash.afterStoreTargetFile(filename, list);
    }

}
