
import java.io.IOException;

public class ControllerTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        Controller controller =new Controller(8000, Config.r, Config.timeout, Config.rebalancedPeriod);
    }
}
