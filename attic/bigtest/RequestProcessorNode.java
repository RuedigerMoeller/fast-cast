package de.ruedigermoeller.fastcast.bigtest;

import de.ruedigermoeller.fastcast.bigtest.services.RequestServer;
import org.nustaq.fastcast.api.FCRemoting;
import org.nustaq.fastcast.api.FastCast;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/14/13
 * Time: 5:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class RequestProcessorNode {

    public static void main( String arg[] ) throws IOException {
        FCRemoting rem = FastCast.getFastCast();
        rem.joinCluster("shared/bigtest.yaml", "Processor", null);
        rem.start("rqserver");
        System.out.println("started "+rem.getNodeId());
        if ( arg.length == 2 ) {
            RequestServer rqserver = (RequestServer) rem.getService("rqserver");
            rqserver.setFilter( Integer.parseInt(arg[0]), Integer.parseInt(arg[1]) );
        }
    }

}
