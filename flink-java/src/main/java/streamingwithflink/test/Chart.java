package io.github.streamingwithflink.test;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.ui.ApplicationFrame;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.Millisecond;

import java.io.IOException;

public class Chart extends ApplicationFrame
{
    DefaultCategoryDataset dataset ;
    public Chart( String applicationTitle , String chartTitle )
    {
        super(applicationTitle);
        dataset = new DefaultCategoryDataset();

        JFreeChart lineChart = ChartFactory.createLineChart(
                chartTitle,
                "Years","Number of Schools",
                createDataset(),
                PlotOrientation.VERTICAL,
                true,true,false);

        ChartPanel chartPanel = new ChartPanel( lineChart );
        chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
        setContentPane( chartPanel );
    }

    public Chart(String title) {
        super(title);
    }

    private DefaultCategoryDataset createDataset( )
    {

        dataset.addValue( 15 , "schools" , "1970" );
        dataset.addValue( 30 , "schools" , "1980" );
        dataset.addValue( 60 , "schools" ,  "1990" );
        dataset.addValue( 120 , "schools" , "2000" );
        dataset.addValue( 240 , "schools" , "2010" );
        dataset.addValue( 300 , "schools" , "2014" );
        return dataset;
    }
    public void run() throws InterruptedException {
        while (true) {
            dataset.addValue(800,"schools" , "2020");
            Thread.sleep(1000);
        }
    }
    public static void main( String[ ] args )
    {
        Chart chart = new Chart(
                "School Vs Years" ,
                "Numer of Schools vs years");

        chart.pack( );

        chart.setVisible( true );
    }
}

