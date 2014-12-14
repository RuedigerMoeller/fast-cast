package bench.latency;

/**
 * Created by ruedi on 07.12.14.
 */
public class FCPongServer {

    public static void main(String arg[]) throws InterruptedException {
        new FCPing().runPongServer();
    }

}
