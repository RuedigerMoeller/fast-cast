package de.ruedigermoeller.fastcast.gui;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.EventHandler;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.chart.*;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TreeItem;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.BorderPane;
import javafx.scene.shape.Rectangle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/29/13
 * Time: 12:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class TopicChart extends BorderPane {

    NumberAxis xAxis;
    NumberAxis yAxis;
    ArrayList<StackedAreaChart.Data> retrans;
    ArrayList<StackedAreaChart.Data> regular;
    private String nodeId;
    Button close;
    private boolean running = true;

    public void init(boolean send, boolean stacked, String nodeId, String title, String xa, String ya) {
        try {
//            setCenter(tree);
            this.nodeId = nodeId;
//            Label titleLabel = new Label("  "+title);
//            setTop(titleLabel);
//            titleLabel.setAlignment(Pos.CENTER);

            int seconds = 300;
            xAxis = NumberAxisBuilder.create()
                    .label(xa)
                    .lowerBound(1.0d)
                    .upperBound(seconds-1)
                    .tickUnit(10.0d).build();

            yAxis = NumberAxisBuilder.create()
                    .label(ya)
                    .lowerBound(0.0d)
                    .upperBound(100000.0d)
                    .tickUnit(100.0d).build();

            retrans = new ArrayList<>();
            for ( int i = 0; i < seconds; i++ ) {
                XYChart.Data d = new StackedAreaChart.Data(i, 0);
                d.setNode(new Rectangle(0,0,0,0));
                retrans.add(d);
            }

            regular = new ArrayList<>();
            for ( int i = 0; i < seconds; i++ ) {
                XYChart.Data e = new StackedAreaChart.Data(i, 0);
                e.setNode(new Rectangle(0,0,0,0));
                regular.add(e);
            }

            ObservableList<StackedAreaChart.Series> areaChartData = FXCollections.observableArrayList(
                new StackedAreaChart.Series("Data (dgrams/sec)", FXCollections.observableArrayList(regular) ),
                new StackedAreaChart.Series("Retransmission (dgrams/sec)",FXCollections.observableArrayList(retrans) )
            );

            String border = "";//-fx-stroke: black; -fx-stroke-width: 2px;";
            if ( stacked ) {
                StackedAreaChart chart = new StackedAreaChart(xAxis, yAxis, areaChartData);
                chart.setAnimated(false);
                chart.setLegendVisible(false);
                chart.setTitle(title);
                setCenter(chart);
                if ( send ) {
                    Node lookup = chart.lookup(".chart");
                    lookup.setStyle("-fx-background-color:  #c0d0c0; "+border);
                } else {
                    Node lookup = chart.lookup(".chart");
                    lookup.setStyle("-fx-background-color:  #ccdccc; "+border);
                }

            } else {
                LineChart chart = new LineChart(xAxis, yAxis, areaChartData);
                chart.setAnimated(false);
                chart.setLegendVisible(false);
                chart.setTitle(title);
                setCenter(chart);
                if ( send ) {
                    Node lookup = chart.lookup(".chart");
                    lookup.setStyle("-fx-background-color:  #c0d0c0; "+border);
                } else {
                    Node lookup = chart.lookup(".chart");
                    lookup.setStyle("-fx-background-color:  #ccdccc; "+border);
                }
            }
            close = new Button("X");
            close.setStyle(
                    "-fx-background-color: "+(send?"#c0d0c0":"#ccdccc")+"; " +
                    "-fx-background-radius: 0; " +
                    "-fx-background-insets: 0;"
            );
            setRight(close);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void shiftAndAddValues(double ret, double reg) {
        shiftAndAdd(retrans,ret);
        shiftAndAdd(regular,reg);
    }

    void shiftAndAdd(List<StackedAreaChart.Data> l, double value) {
        for (int i = l.size()-2; i >= 0; i--) {
            l.get(i+1).setYValue(l.get(i).getYValue());
        }
        l.get(0).setYValue(value);
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
