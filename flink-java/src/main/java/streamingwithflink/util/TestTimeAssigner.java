package io.github.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<TestReading> {

    public TestTimeAssigner() {
        super(Time.seconds(10));
    }

    @Override
    public long extractTimestamp(TestReading testReading) {
        return testReading.timestamp;
    }
}
