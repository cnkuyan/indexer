package com.cuyan.indexer.model;

import java.util.Objects;

public class Tick {
    protected String instrument;

    protected double price;

    protected long timestamp;

    /**
     *
     */
    public Tick() {
        super();
    }

    /**
     *
     * @param instrument
     * @param price
     * @param timestamp
     */
    public Tick(String instrument, double price, long timestamp) {
        this.instrument = instrument;
        this.price = price;
        this.timestamp = timestamp;
    }

    public String getInstrument() {
        return instrument;
    }

    public double getPrice() {
        return price;
    }

    public long getTimestamp() {
        return timestamp;
    }



    @Override
    public String toString() {
        return "Tick{" +
                "instrument='" + instrument + '\'' +
                ", price=" + price +
                ", timestamp=" + timestamp +
                '}';
    }

}
