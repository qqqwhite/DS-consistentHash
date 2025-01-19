import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.logging.Logger;

/**
 * 对socket进行封装，简化通信流程
 */
public class Communication {
    private static final Logger logger = Logger.getLogger(Communication.class.getName());
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private int port;
    private int timeout;
    public CommunicationStatus status;

    /**
     * 初始化
     * @param port
     * @param timeout
     * @throws IOException
     */
    public Communication(int port, int timeout) throws IOException {
        this.port = port;
        try {
            socket = new Socket(InetAddress.getLoopbackAddress(), port);
            socket.setSoTimeout(timeout);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            status = CommunicationStatus.ESTABLISHED;
            logger.info(String.format("established communication on %d", port));
        } catch (IOException e) {
            status = CommunicationStatus.ERROR;
            throw new IOException("connect error with establishing");
        }
    }

    /**
     * 初始化
     * @param port
     * @throws IOException
     */
    public Communication(int port) throws IOException {
        this.port = port;
        try {
            socket = new Socket(InetAddress.getLoopbackAddress(), port);
//            socket.setSoTimeout(Integer.MAX_VALUE);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            status = CommunicationStatus.ESTABLISHED;
            logger.info(String.format("established communication on %d", port));
        } catch (IOException e) {
            status = CommunicationStatus.ERROR;
            throw new IOException("connect error with establishing");
        }
    }

    /**
     * 初始化
     * @param socket
     * @param port
     * @throws IOException
     */
    public Communication(Socket socket, int port) throws IOException {
        this.port = port;
        this.socket = socket;
//        this.socket.setSoTimeout(timeout);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        this.status = CommunicationStatus.ESTABLISHED;
    }

    /**
     * 初始化
     * @param socket
     * @param port
     * @param timeout
     * @throws IOException
     */
    public Communication(Socket socket, int port, int timeout) throws IOException {
        this.port = port;
        this.socket = socket;
        this.socket.setSoTimeout(timeout);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        this.status = CommunicationStatus.ESTABLISHED;
    }

    /**
     * 发生信息
     * @param msg
     * @throws IOException
     */
    public void send(String msg) throws IOException {
        if (status == CommunicationStatus.ESTABLISHED) {
            out.print(msg + "\n");
            out.flush();
            logger.info(String.format("send message: %s", msg));
            return;
        }
        socket.close();
        throw new IOException("connect error");
    }

    /**
     * 接收信息
     * @return
     * @throws IOException
     */
    public String receive() throws IOException {
        if (status == CommunicationStatus.ESTABLISHED) {
            try {
                String msg = in.readLine();
                logger.info(String.format("receive message: %s", msg));
                return msg;
            } catch (SocketTimeoutException e){
                throw new SocketTimeoutException("receive timeout");
            } catch (IOException e) {
                socket.close();
                status = CommunicationStatus.ERROR;
                throw new IOException("connect error with receiving");
            }
        }
        socket.close();
        throw new IOException("connect error");
    }

    /**
     * 发生文件内容
     * @param content
     * @throws IOException
     */
    public void sendContent(byte[] content) throws IOException {
        if (status == CommunicationStatus.ESTABLISHED) {
            socket.getOutputStream().write(content);
            socket.getOutputStream().flush();
            return;
        }
        socket.close();
        throw new IOException("connect error");
    }

    /**
     * 接收文件内容
     * @param filesize
     * @return
     * @throws IOException
     */
    public byte[] receiveContent(int filesize) throws IOException {
        if (status == CommunicationStatus.ESTABLISHED) {
            try {
                byte[] buffer = new byte[filesize];
                socket.getInputStream().readNBytes(buffer, 0, filesize);
                if (in.read() != -1){
                    throw new IOException("file transfer error");
                }
                return buffer;
            } catch (IOException e) {
                socket.close();
                status = CommunicationStatus.ERROR;
                throw new IOException("connect error with receiving");
            }
        }
        socket.close();
        throw new IOException("connect error");
    }

    /**
     * 关闭通信
     * @throws IOException
     */
    public void close() throws IOException {
        try {
            in.close();
            out.close();
            socket.close();
            status = CommunicationStatus.CLOSE;
            logger.info(String.format("close communication on %d", port));
        } catch (IOException e) {
            throw new IOException("connect error with closing");
        }
    }

    public boolean isClosed(){
        return socket.isClosed() || !socket.isConnected();
    }

    public int getTargetPort(){
        return socket.getPort();
    }
}
