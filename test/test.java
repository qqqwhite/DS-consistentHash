import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args) throws InterruptedException {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0;i<10;i++){
            new Thread() {
                @Override
                public void run() {
                    list.add(1);
                }
            }.start();
        }
        Thread.sleep(100);
        System.out.println(list.size());
    }
}
