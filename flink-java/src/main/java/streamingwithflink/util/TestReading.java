package io.github.streamingwithflink.util;

public class TestReading {
    // id of the car
    public String id;
    // timestamp of the reading
    public long timestamp;
    // x value of the reading
    public double x;
    // y value of the reading
    public double y;

    /**
     * Empty default constructor to satify Flink's POJO requirements.
     */
    public TestReading() {
    }

    public TestReading(String id, long timestamp, double x, double y) {
        this.id = id;
        this.timestamp = timestamp;
        this.x = x;
        this.y = y;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.x + ", " + this.y + ")";
    }
}
