package de.ruedigermoeller.fastcast.gui;

import org.nustaq.fastcast.config.FCClusterConfig;
import org.nustaq.fastcast.config.FCSubscriberConf;
import org.nustaq.fastcast.remoting.FastCast;
import org.nustaq.fastcast.service.FCMembership;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Side;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.effect.GaussianBlur;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/28/13
 * Time: 7:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class ClusterViewApp extends Application implements FCMembership.MemberShipListener {

    public static ClusterViewApp App;

    FCMembership remoteMembership, localMembership;

    @Override
    public void start(Stage primaryStage) throws Exception {

        App = this;
        BorderPane root = new BorderPane();

        Scene scene = new Scene(root);
        try {
        scene.getStylesheets().addAll(getClass().getResource("/css/clusterview.css").toExternalForm());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        primaryStage.setScene(scene);
        primaryStage.setTitle("FastCast Cluster View");
        primaryStage.setOnCloseRequest(new EventHandler<WindowEvent>() {
            @Override
            public void handle(WindowEvent windowEvent) {
                Platform.exit();
                System.exit(-1);
            }
        });

        final TabPane tabPane = new TabPane();
        tabPane.setSide(Side.TOP);
        tabPane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);

        final Tab charts = createNodesTab();
//        final Tab services = createServiceTab();

        tabPane.getTabs().addAll(charts);

//        root.setCenter(tabPane);
//        root.setMargin(tabPane, new Insets(8,6,4,6));
        root.setCenter(charts.getContent());
        root.setMargin(charts.getContent(), new Insets(8,6,4,6));

        root.setTop(createTopPane());

        primaryStage.setWidth(900);
        primaryStage.setHeight(600);
        primaryStage.show();
        if ( sArgs.length > 0 ) {
            new Thread() {
                public void run() {initCluster(sArgs[0]);}
            }.start();
        } else {
            System.out.println("please provide a full path to cluster.yaml definition file");
            System.exit(-1);
        }

    }

    NodesView clGui;

    private Tab createNodesTab() {
        Tab t = new Tab();
        t.setText("Nodes");
        clGui = new NodesView();
        clGui.init();
        t.setContent(clGui);
        return t;
    }

    private Tab createServiceTab() {
        Tab t = new Tab();
        t.setText("Discovery");
//        try {
//            FGDiscoveryGUI gui = new FGDiscoveryGUI();
//            gui.init();
//            t.setContent(gui);
//            FGClusterDiscovery discovery = (FGClusterDiscovery) node.getExposedService(IFGClusterDiscovery.class);
//            gui.setDiscovery(discovery);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        return t;
    }

    private Node createTopPane() {
        BorderPane canv = new BorderPane();
        Label fast = new Label(".  . .FastCAST");
        fast.setStyle(
//                "-fx-text-fill:darkorange;\n" +
                "    -fx-alignment: center-right; " +
                        "    -fx-padding: 5 10 5 10;\n" +
                        "    -fx-font-family: \"Verdana\";\n" +
                        "    -fx-stroke: black;" +
                        "    -fx-stroke-width: 1;" +
                        "    -fx-font-size: 25px;\n" +
                        "    -fx-font-weight: bold;\n" +
                        "    -fx-text-fill: black;" +
                        "\n" +
                        "    -fx-background-insets: 0,1,4,5,6;\n" +
                        "    -fx-background-radius: 9,8,5,4,3;"
        );
        fast.setEffect(new GaussianBlur(2));

//        canv.setStyle(
//                " -fx-background-color:linear-gradient(from 60% 0% to 100% 100%, white 0%, #709070 100%);"
//        );

        canv.setRight(fast);
        return canv;
    }

    private void initCluster(String yamlFinam) {
        if ( yamlFinam == null || !new File(yamlFinam).exists() ) {
            System.out.println("file does not exits " + yamlFinam);
            System.exit(0);
        }
        try {
            FCSubscriberConf toJoin = null;
            FCClusterConfig fcClusterConfig = FCClusterConfig.read(yamlFinam);
            FCSubscriberConf[] topics = (FCSubscriberConf[]) fcClusterConfig.getTopics();
            for (int i = 0; i < topics.length; i++) {
                FCSubscriberConf topic = topics[i];
                if ( topic.getServiceClass() != null && topic.getServiceClass().equals(FCMembership.class.getName()) ) {
                    toJoin = topic;
                    break;
                }
            }
            if ( toJoin == null ) {
                System.out.println("no FCMembership Service found");
                System.exit(0);
            }
            FastCast.getFastCast().joinCluster(yamlFinam, "gui", null);
            if (!toJoin.isAutoStart()) {
                FastCast.getFastCast().start(toJoin.getName());
            }
            localMembership = (FCMembership) FastCast.getFastCast().getService(toJoin.getName());
            localMembership.setDoLogClusterMessages(true);
            remoteMembership = (FCMembership) FastCast.getFastCast().getRemoteService(toJoin.getName());
            localMembership.setListener(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String sArgs[];
    public static void main(String[] args) {
        sArgs = args;
        launch(args);
    }

    @Override
    public void nodeAdded(String sender, Object state) {
        clGui.nodeAdded(sender,state);
    }

    @Override
    public void nodeLost(String nodeId) {
        clGui.nodeLost(nodeId);
    }
}
