package de.ruedigermoeller.fastcast.gui;

import de.ruedigermoeller.fastcast.packeting.TopicStats;
import de.ruedigermoeller.fastcast.remoting.FCFutureResultHandler;
import de.ruedigermoeller.fastcast.remoting.FCSendContext;
import de.ruedigermoeller.fastcast.service.FCMembership;
import javafx.application.Platform;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Orientation;
import javafx.scene.control.*;
import javafx.scene.control.cell.TextFieldTreeCell;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;
import javafx.util.Callback;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/28/13
 * Time: 8:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class NodesView extends BorderPane implements FCMembership.MemberShipListener {

    TreeView tree = new TreeView();
    TreeItem root;
    VBox charts;
    ContextMenu topicMenu;

    public void init() {
        try {
            tree.setCellFactory(new Callback<TreeView, TreeCell>() {
                @Override
                public TreeCell call(TreeView arg0) {
                    // custom tree cell that defines a context menu for the root tree item
                    return new TextFieldTreeCell() {
                        @Override
                        public void updateItem(Object s, boolean b) {
                            super.updateItem(s, b);
                            if (getSelectedPath(getTreeItem()).size() == 2) {
                                setContextMenu(topicMenu);
                            } else
                                setContextMenu(null);
                        }
                    };
                }
            });

            root = new TreeItem(".");
            tree.setRoot(root);
            tree.setShowRoot(false);
            root.setExpanded(true);
            charts = new VBox(4);
            ScrollPane sp = new ScrollPane();
            sp.setContent(charts);

            SplitPane split = new SplitPane();
            split.getItems().add(tree);
            split.getItems().add(sp);

            setCenter(split);
            split.setOrientation(Orientation.HORIZONTAL);
            split.setDividerPositions(.18);

            topicMenu = buildTopicPopup();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createAndAddChart(final ChartType type) {
        TreeItem selectedItem = (TreeItem) tree.getSelectionModel().getSelectedItem();
        ArrayList<TreeItem> path = getSelectedPath(selectedItem);
        if (path.size() == 2) {
            final TopicChart chart = new TopicChart();
            String nodeid = path.get(1).getValue().toString();
            final String topic = (String) path.get(0).getValue();
            if (path.get(1).getValue() instanceof FCMembership.MemberNodeInfo) {
                nodeid = ((FCMembership.MemberNodeInfo) path.get(1).getValue()).getNodeId();
            }
            chart.init( type.isSend(), type.isStacked(), nodeid, type.getTitlePrefix() + nodeid + ", " + path.get(0).getValue(), "seconds ago", type.getYLabel());
            addChart(chart);
            final String finalNodeid = nodeid;
            new Thread("Chart Data") {
                public void run() {
                    while (chart.isRunning()) {
                        try {
                            Thread.sleep(1000);
                            FCSendContext.get().setReceiver(finalNodeid);
                            ClusterViewApp.App.remoteMembership.getStats(topic, new FCFutureResultHandler() {
                                @Override
                                public void resultReceived(Object obj, String sender) {
                                    TopicStats stats = (TopicStats) obj;
                                    if ( stats.getRecordEnd()-stats.getRecordStart() < 500 ) {
                                        System.out.println("skipped stats duration:"+(stats.getRecordEnd()-stats.getRecordStart()));
                                        return;
                                    }
//                                    System.out.println(stats);
                                    switch (type) {
                                        case SEND_RETRANS:
                                            chart.shiftAndAddValues(stats.getPacketsSentPerSecond(), stats.getPacketsRetransSentPerSecond());
                                            break;
                                        case SEND_MSG:
                                            chart.shiftAndAddValues(stats.getMsgSent(), stats.getPacketsSentPerSecond());
                                            break;
                                        case SEND_BW:
                                            chart.shiftAndAddValues(stats.getBytesSentPerSecond() / 1000.0 / 1000.0, 0);
                                            break;
                                        case REC_MSG:
                                            chart.shiftAndAddValues( stats.getMsgReceived(), stats.getPacketsRecPerSecond() );
                                            break;
                                        case REC_RETRANS:
                                            double packetsSentPerSecond = stats.getPacketsRecPerSecond();
                                            chart.shiftAndAddValues(packetsSentPerSecond, stats.getRetransReq());
                                            break;
                                        case REC_BW:
                                            chart.shiftAndAddValues(stats.getBytesRecPerSecond() / 1000.0 / 1000.0, 0);
                                            break;
                                    }
                                    done();
                                }
                            });
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();
        }
    }

    private ArrayList<TreeItem> getSelectedPath(TreeItem selectedItem) {
        if ( selectedItem == null )
        {
            return new ArrayList<>();
        }
        ArrayList<TreeItem> path = new ArrayList<>();
        if ( selectedItem == null )
            return path;
        while( selectedItem != root ) {
            path.add(selectedItem);
            selectedItem = selectedItem.getParent();
        }
        return path;
    }

    enum ChartType {
        SEND_RETRANS(true,"Regular/Retransmission Dgrams sent from ", "datagrams", true),
        SEND_MSG(false, "Messages/Datagrams sent from ", "msg / dgrams", true),
        SEND_BW(false,"Traffic sent by ", "MBytes", true),
        REC_RETRANS(true,"Dgrams received vs. Retrans Req sent from ", "dgrams/req sent", false),
        REC_MSG(false, "Msg received/Dgrams received from ", "msg / dgrams", false),
        REC_BW(false, "Traffic received by ", "MBytes", false);

        boolean stacked;
        String titlePrefix;
        private String YLabel;
        boolean isSend;

        ChartType(boolean stacked, String titlePrefix, String ya, boolean send) {
            this.stacked = stacked;
            this.titlePrefix = titlePrefix;
            YLabel = ya;
            isSend = send;
        }

        boolean isSend() {
            return isSend;
        }

        boolean isStacked() {
            return stacked;
        }

        String getTitlePrefix() {
            return titlePrefix;
        }

        public String getYLabel() {
            return YLabel;
        }

        public void setYLabel(String YLabel) {
            this.YLabel = YLabel;
        }
    }

    ContextMenu buildTopicPopup() {
        final ContextMenu contextMenu = new ContextMenu();

        MenuItem senderChart = new MenuItem("Show Send DGram Stats");
        senderChart.setOnAction(new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {
                createAndAddChart(ChartType.SEND_RETRANS);
            }
        });
        MenuItem sendMsgChart = new MenuItem("Show Send Msg Stats");
        sendMsgChart.setOnAction(new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {
                createAndAddChart(ChartType.SEND_MSG);
            }
        });
        MenuItem sendBWChart = new MenuItem("Show Send Bandwidth Stats");
        sendBWChart.setOnAction(new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {
                createAndAddChart(ChartType.SEND_BW);
            }
        });

        MenuItem recChart = new MenuItem("Show Receive Msg Stats");
        recChart.setOnAction(new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {
                createAndAddChart(ChartType.REC_MSG);
            }
        });

        MenuItem recDGChart = new MenuItem("Show Receive DGram Stats");
        recDGChart.setOnAction(new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {
                createAndAddChart(ChartType.REC_RETRANS);
            }
        });

        MenuItem recBWChart = new MenuItem("Show Receive Bandwith Stats");
        recBWChart.setOnAction(new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {
                createAndAddChart(ChartType.REC_BW);
            }
        });

        contextMenu.getItems().addAll(senderChart,sendMsgChart,sendBWChart, recDGChart, recChart, recBWChart );
        return contextMenu;
    }

    public void addChart(final TopicChart chart) {
        chart.setMaxHeight(200);
        chart.setPrefHeight(200);
        chart.close.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent mouseEvent) {
                chart.setRunning(false);
                charts.getChildren().remove(chart);
            }
        });
        charts.getChildren().add(0,chart);
    }

    public void remNodeChart(String nodeid) {
        for (int i = 0; i < charts.getChildren().size(); i++) {
            TopicChart node = (TopicChart) charts.getChildren().get(i);
            if ( node.getNodeId().equals(nodeid) ) {
                charts.getChildren().remove(node);
                node.setRunning(false);
                i--;
            }
        }
    }

    @Override
    public synchronized void nodeAdded(final String sender, Object state) {
        Platform.runLater( new Runnable() {
            @Override
            public void run() {
                final TreeItem item = new TreeItem(sender);
                root.getChildren().add(item);
                FCSendContext.get().setReceiver(sender);
                ClusterViewApp.App.remoteMembership.getNodeInfo(new FCFutureResultHandler() {
                        @Override
                        public void resultReceived(final Object obj, String sender) {
                            Platform.runLater(new Runnable() {
                                @Override
                                public void run() {
                                    item.setValue(obj);
                                    done();
                                }
                            });
                        }
                    }
                );
                FCSendContext.get().setReceiver(sender);
                ClusterViewApp.App.remoteMembership.getActiveTopics(new FCFutureResultHandler() {
                        @Override
                        public void resultReceived(final Object obj, final String sender) {
                            Platform.runLater(new Runnable() {
                                @Override
                                public void run() {
                                    TreeItem nodeItem = findNodeItem(sender);
                                    nodeItem.getChildren().add(new TreeItem(obj));
                                }
                            });
        //                    Collections.sort(nodeItem.getChildren(), new Comparator() {
        //                        @Override
        //                        public int compare(Object o1, Object o2) {
        //                            return o1.toString().compareTo(o2.toString());
        //                        }
        //                    });
                        }
                    }
                );
            }
        });
    }

    TreeItem findNodeItem(String id) {
        for (int i = 0; i < root.getChildren().size(); i++) {
            TreeItem o = (TreeItem) root.getChildren().get(i);
            if (o.getValue().equals(id) ) {
                return o;
            }
            if ( o.getValue() instanceof FCMembership.MemberNodeInfo && ((FCMembership.MemberNodeInfo) o.getValue()).getNodeId().equals(id) ) {
                return o;
            }
        }
        return null;
    }

    @Override
    public  void nodeLost(final String nodeId) {
        Platform.runLater(new Runnable() {
            @Override
            public void run() {
                synchronized (NodesView.this) {
                    tree.getSelectionModel().clearSelection();
                    root.getChildren().remove(findNodeItem(nodeId));
                    remNodeChart(nodeId);
                }
            }
        });
    }

}
