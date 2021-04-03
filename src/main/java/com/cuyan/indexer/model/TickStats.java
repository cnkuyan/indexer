package com.cuyan.indexer.model;


import com.fasterxml.jackson.annotation.JsonProperty;

public class TickStats {

    @JsonProperty
    private String instrument;
    @JsonProperty
    private double min;
    @JsonProperty
    private double max;
    @JsonProperty
    private double avg;
    @JsonProperty
    private long count;


    public TickStats() {
        super();
    }

    public TickStats(String instrument, double min, double max, double avg, long count) {
        this.instrument = instrument;
        this.min = min;
        this.max = max;
        this.count = count;
   }

    public String getInstrument() {
        return instrument;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAvg() {
        return avg;
    }

    public long getCount()
    {
        return count;
    }

    public void setInstrument(String instrument) {
        this.instrument = instrument;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public void setCount(long count)
    {
        this.count = count;
    }

}
