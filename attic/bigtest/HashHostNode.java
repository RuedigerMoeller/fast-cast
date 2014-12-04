package de.ruedigermoeller.fastcast.bigtest;

import org.nustaq.fastcast.api.FCRemoting;
import org.nustaq.fastcast.api.FastCast;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/14/13
 * Time: 5:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class HashHostNode {

    public static void main( String arg[] ) throws IOException {
        FCRemoting rem = FastCast.getFastCast();
        rem.joinCluster("shared/bigtest.yaml", "HTHost", null);
        rem.startSending("htlisten");
        rem.start("hthost");
        System.out.println("started "+rem.getNodeId());
    }

}
