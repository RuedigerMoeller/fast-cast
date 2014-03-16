package de.ruedigermoeller.fastcast.config;

import de.ruedigermoeller.fastcast.remoting.FCEmptyService;
import de.ruedigermoeller.fastcast.transport.FCSocketConf;
import de.ruedigermoeller.fastcast.util.FCLog;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/5/13
 * Time: 5:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class FCClusterConfig {

    String clusterName = "def";

    HashMap<String,String> interfaces = new HashMap<>();
    FCSocketConf transports[] = new FCSocketConf[] { new FCSocketConf("default"), new FCSocketConf("control") };
    FCTopicConf [] topics  = new FCTopicConf[] { new FCTopicConf("defaulttopic","default",0,FCEmptyService.class.getName()), new FCTopicConf("controlchannel","default",0,FCEmptyService.class.getName()) };

    int logLevel = FCLog.WARN;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getLogLevel() {
        return logLevel;
    }

    public void defineInterface(String symbolic, String realNameOrIp) {
        interfaces.put(symbolic,realNameOrIp);
    }

    public HashMap<String, String> getInterfaces() {
        return interfaces;
    }

    public void setInterfaces(HashMap<String, String> interfaces) {
        this.interfaces = interfaces;
    }

    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }

    public FCSocketConf[] getTransports() {
        for (int i = 0; i < transports.length; i++) {
            FCSocketConf transport = transports[i];
            if ( interfaces.get(transport.getIfacAdr()) != null ) {
                transport.setIfacAdr(interfaces.get(transport.getIfacAdr()));
            }
        }
        return transports;
    }

    public void setTransports(FCSocketConf[] transports) {
        this.transports = transports;
    }

    public FCTopicConf[] getTopics() {
        return topics;
    }

    public void setTopics(FCTopicConf[]topics ) {
        this.topics = topics;
    }

    public void write(String finam) throws IOException {
        DumperOptions opt = new DumperOptions();
        opt.setPrettyFlow(true);
        Representer representer = new Representer();
//        representer.addClassTag(FCTopicConf.class, new Tag("!topic"));
//        representer.addClassTag(FCSocketConf.class, new Tag("!socket"));
        Yaml yaml = new Yaml(representer,opt);
        FileWriter wri = new FileWriter(finam);
        wri.write(yaml.dumpAsMap(this));
        wri.close();
    }

    public static void write(String finam,FCClusterConfig data) throws IOException {
        DumperOptions opt = new DumperOptions();
        opt.setPrettyFlow(true);
        Representer representer = new Representer();
        representer.addClassTag(FCTopicConf.class, new Tag("!topic"));
        representer.addClassTag(FCSocketConf.class, new Tag("!socket"));
        Yaml yaml = new Yaml(representer,opt);
//        FileWriter wri = new FileWriter(finam);
//        wri.write(yaml.dumpAsMap(data));
//        wri.close();
        System.out.println(yaml.dumpAsMap(data));
    }

    public static FCClusterConfig read(InputStream in) throws IOException {
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
        FCClusterConfig conf = (FCClusterConfig) yaml.loadAs(in, FCClusterConfig.class);
        in.close();
        return conf;
    }

    public void overrideTransBy( FCLocalClusterConf c ) {
        if ( c == null )
            return;
        HashMap[] overTrans = (HashMap[]) c.getTransports();
        if ( overTrans == null ) {
            return;
        }
        HashMap patched = new HashMap();
        for (int i = 0; i < overTrans.length; i++) {
            HashMap hashMap = overTrans[i];
            String id = (String) hashMap.get("name");
            if ( id == null ) {
                throw new RuntimeException("missing attribute name on localconfig transport definition");
            }
            patched.put(id,hashMap);
        }
        for (int i = 0; i < transports.length; i++) {
            FCSocketConf transport = transports[i];
            HashMap oo = (HashMap) patched.get(transport.getName());
            if ( oo != null ) {
                Field[] fields = transport.getClass().getDeclaredFields();
                for (int j = 0; j < fields.length; j++) {
                    Field field = fields[j];
                    Object val = oo.get(field.getName());
                    if ( val != null ) {
                        field.setAccessible(true);
                        try {
                            field.set(transport,val);
                            FCLog.log("override " + val + " field " + field.getName() + " trans " + transport.getName());
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    /**
     * overrides this with settings found in file. afterwards looks for ../local/[filename].yaml and
     * overrides again with those settings
     * @param configFile
     */
    public FCClusterConfig overrideBy(String configFile) {
        try {
            if ( new File("shared"+File.separator+configFile).exists() )
            {
                configFile = "shared"+File.separator+configFile;
            }
            if ( new File(configFile).exists() ) {
                FCLocalClusterConf local = null;
                    local = FCLocalClusterConf.read(configFile);
                FCLog.log("overriding config with "+new File(configFile).getAbsolutePath());
                overrideBy(local);
            }

            String localover = new File(configFile).getAbsoluteFile().getParentFile().getParent()+File.separator+"local"+File.separator+new File(configFile).getName();
            if ( ! new File(localover).exists() ) {
                localover = "."+File.separator+"local"+File.separator+configFile;
            }
            if ( new File(localover).exists() ) {
                FCLocalClusterConf local = FCLocalClusterConf.read(localover);
                FCLog.log("overriding config with local "+new File(localover).getAbsolutePath());
                overrideBy(local);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void overrideBy(FCLocalClusterConf over) {
        overrideIfaceBy(over);
        overrideTransBy(over);
        overrideTopicBy(over);
    }

    void overrideIfaceBy( FCLocalClusterConf c ) {
        if ( c == null )
            return;
        HashMap hm = c.getInterfaces();
        for (Iterator iterator = hm.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry next = (Map.Entry) iterator.next();
            interfaces.put((String) next.getKey(), (String) next.getValue());
        }
    }

    void overrideTopicBy( FCLocalClusterConf c ) {
        if ( c == null )
            return;
        HashMap[] overTopic = (HashMap[]) c.getTopics();
        if ( overTopic == null ) {
            return;
        }
        HashMap patched = new HashMap();
        for (int i = 0; i < overTopic.length; i++) {
            HashMap hashMap = overTopic[i];
            String id = (String) hashMap.get("name");
            if ( id == null ) {
                throw new RuntimeException("missing attribute name on localconfig transport definition");
            }
            patched.put(id,hashMap);
        }
        for (int i = 0; i < topics.length; i++) {
            FCTopicConf topic = topics[i];
            HashMap oo = (HashMap) patched.get(topic.getName());
            if ( oo != null ) {
                // sepcial: [datagrams]-[historyseconds] e.g. 12000-5
                if ( oo.get("rateByDGrams") != null ) {
                    String s = (String) oo.get("rateByDGrams");
                    String[] split = s.split("-");
                    if ( split.length != 2 )
                        throw new RuntimeException("illegal format in rateByDGrams directive. Use e.g. 10000-5");

                    int maxDatagramsPerSecond = Integer.parseInt(split[0]);
                    int maxGCPauseSeconds = Integer.parseInt(split[1]);
                    FCLog.log("set rate by DGrams:"+maxDatagramsPerSecond+" "+maxGCPauseSeconds);
                    //FIXME: copied from builder
                    topic.setDGramRate(maxDatagramsPerSecond);
                    // do 250 ms history on heap
                    topic.setNumPacketHistory( Math.max(maxDatagramsPerSecond*maxGCPauseSeconds,500) );
                    topic.setReceiveBufferPackets(maxDatagramsPerSecond);
                }
                Field[] fields = topic.getClass().getDeclaredFields();
                for (int j = 0; j < fields.length; j++) {
                    Field field = fields[j];
                    Object val = oo.get(field.getName());
                    if ( val != null ) {
                        field.setAccessible(true);
                        try {
                            field.set(topic,val);
                            FCLog.log("override " + val + " field " + field.getName() + " topic " + topic.getName());
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    public static FCClusterConfig read(String finam) throws IOException {
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
        if ( ! new File(finam).exists() ) {
            finam = "shared"+File.separator+finam;
        }
        FileReader reader = new FileReader(finam);
        FCClusterConfig conf = (FCClusterConfig) yaml.loadAs(reader, FCClusterConfig.class);
        reader.close();
        return conf;
    }

    public static void main(String arg[]) throws IOException {
        FCClusterConfig conf = new FCClusterConfig();conf.interfaces.put("lo", "127.0.0.1");
        write("/local/moelrue/work/fst/fastcast/build/control/fccontrol.yaml", conf);
//        String override = "/local/moelrue/work/fst/fastcast/build/control/fccontrol.yaml";
//        String finam = "/local/moelrue/work/fst/fastcast/test/bench.yaml";
//
//        conf = read(finam);
//
//        FCLocalClusterConf over = FCLocalClusterConf.read(override);
//        conf.overrideBy(over);
    }

}
