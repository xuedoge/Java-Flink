package io.github.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class TestSource extends RichParallelSourceFunction<TestReading> {
    // flag indicating whether source is still running
    private boolean running = true;

    /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
    @Override
    public void run(SourceContext<TestReading> srcCtx) throws Exception {

        // initialize random number generator
        Random rand = new Random();
        // look up index of this parallel task
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();



        // initialize sensor ids and temperatures
        String[] carIds = new String[10];
        double[] curX = new double[10];
        double[] curY = new double[10];
        for (int i = 0; i < 10; i++) {
            carIds[i] = "car_" + (taskIdx * 10 + i);
            //carIds[i] = "car_" + taskIdx;
            curX[i] = rand.nextGaussian() * 30;
            curY[i] = rand.nextGaussian() * 30;
        }

        while (running) {

            // get current time
            long curTime = Calendar.getInstance().getTimeInMillis();

            // emit SensorReadings
            for (int i = 0; i < 10; i++) {
                // update current temperature.
                curX[i] += rand.nextGaussian();

                // emit reading
                srcCtx.collect(new TestReading(carIds[i], curTime, curX[i], curY[i]));
            }

            // pre second
            Thread.sleep(1000);
        }
    }

    /** Cancels this SourceFunction. */
    @Override
    public void cancel() {
        this.running = false;
    }
}
