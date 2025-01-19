/**
 * 配置类，用于测试时统一修改配置，简化测试流程，实际运行时不会用到
 */
public class Config {
    public static int timeout = 1000;

    public static int r = 3;

    public static int rebalancedPeriod = 3000000;

    public static int virtualDstoreCount = 10000; // 虚拟节点个数

    public static boolean skipRebalanced = false;
}
