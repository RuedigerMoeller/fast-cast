package de.ruedigermoeller.fastcast.config;

import de.ruedigermoeller.fastcast.transport.FCSocketConf;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 9/17/13
 * Time: 4:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class FCLocalClusterConf {

    HashMap<String,String> interfaces = new HashMap<>();
    HashMap transports[];
    HashMap [] topics;

    public HashMap<String, String> getInterfaces() {
        return interfaces;
    }

    public void setInterfaces(HashMap<String, String> interfaces) {
        this.interfaces = interfaces;
    }

    public HashMap[] getTransports() {
        return transports;
    }

    public void setTransports(HashMap[] transports) {
        this.transports = transports;
    }

    public HashMap[] getTopics() {
        return topics;
    }

    public void setTopics(HashMap[] topics) {
        this.topics = topics;
    }

    public static FCLocalClusterConf read(String finam) throws IOException {
        Yaml yaml = new Yaml(new Constructor(){
            @Override
            protected Class<?> getClassForNode(Node node) {
                String name = node.getTag().getValue();
                if ( "!topic".equals(name)) {
                    return FCTopicConf.class;
                }
                if ( "!socket".equals(name) ) {
                    return FCSocketConf.class;
                }
                return super.getClassForNode(node);
            }
        });
        FileReader reader = new FileReader(finam);
        FCLocalClusterConf conf = (FCLocalClusterConf) yaml.loadAs(reader, FCLocalClusterConf.class);
        reader.close();
        return conf;
    }

}
