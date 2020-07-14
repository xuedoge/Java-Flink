package io.github.streamingwithflink.util;

public class CarBean {
    // id of the car
    public String id;
    // timestamp of the reading
    public long timestamp;
    // distance of the car one day
    public double distance;

    public CarBean(){}

    public CarBean(String id, long timestamp, double distance){
        this.id=id;
        this.timestamp=timestamp;
        this.distance=distance;
    }

    public double getDistance() {
        return distance;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getId() {
        return id;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String distanceString(){
        String string = "";
        string += this.distance;
        return string;
    }
    public String idString(){
        String string = "";
        string += this.id;
        return string;
    }
    public String timeString(){
        String string = "";
        string += this.timestamp;
        return string;
    }
}
