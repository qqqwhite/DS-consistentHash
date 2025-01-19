/**
 * 一致性hash算法错误类
 */
public class ConsistentHashException extends Exception{
    public ConsistentHashException(String s) {
        super(s);
    }
}
