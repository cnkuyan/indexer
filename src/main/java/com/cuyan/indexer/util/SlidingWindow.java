package com.cuyan.indexer.util;

import com.cuyan.indexer.model.Ticker;
import com.cuyan.indexer.model.TickStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.*;

import static com.cuyan.indexer.IndexerApplication.*;

public class SlidingWindow implements Runnable{

    Logger logger = LoggerFactory.getLogger(SlidingWindow.class);

    private long windowMillis = 0;
    private long timerRefreshMSecs = 0;
    private boolean isRunning = false;

    private Deque<Ticker> tickArrivals = null;

    private Map<String, TreeSet<Ticker>> tickerMap = null;
    private Map<String, TickStats> tickerStatsMap = null;
    private Map<String, PriorityQueue<Ticker>> tickerMinPQMap;
    private Map<String, PriorityQueue<Ticker>> tickerMaxPQMap;
    private Map<String, Double> tickerTotalPriceMap;
    private Map<String, Long> tickerTotalCountMap;

    private Object lock = new Object();
    private Object stats_lock = new Object();
    private Thread t = null;
    private Timer pruneTimer = null;


    public SlidingWindow(int bufferSize, long aWindowMsecs, long aTimerRefreshMSecs) {
        windowMillis = aWindowMsecs;
        timerRefreshMSecs= aTimerRefreshMSecs;
        tickArrivals = new ArrayDeque<>(bufferSize);

        initMaps();

    }

    /**
     *
     */
    public void start() {

        t = new Thread(this);
        t.start();
        isRunning = true;
        logger.debug("SW Thread started!");
        startTimer(timerRefreshMSecs);
    }

    /**
     *
     */
    public void stop() {
        logger.debug("SlidingWindow stop() called!");

        isRunning = false;
        pruneTimer.cancel();
    }

    /**
     *
     * @param aPlainTick
     */
    public synchronized void add(Ticker aPlainTick) {

        while (tickArrivals.size() == MAX_QUEUE_SIZE)  {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        tickArrivals.offerLast(aPlainTick);
        logger.debug(aPlainTick + "added to tickDeq!");

        notify();

    }

    /**
     *
     */
    private void initMaps() {

        tickerMap = new HashMap<String, TreeSet<Ticker>>();
        tickerStatsMap = new HashMap<>();
        tickerMinPQMap = new HashMap<String, PriorityQueue<Ticker>>();
        tickerMaxPQMap = new HashMap<String, PriorityQueue<Ticker>>();
        tickerTotalPriceMap = new HashMap<>();
        tickerTotalCountMap = new HashMap<>();
    }

    /**
     *
     * @param aPeriod
     */
    private void startTimer(long aPeriod) {

        pruneTimer = new Timer();
        PrunerTask st = new PrunerTask(this);
        pruneTimer.schedule(st, 0, aPeriod);
    }



    /**
     *  Consumes a PriceEqualTick from the head of the queue upon waking
     *  Blocks if the queue is empty,
     *
     *  Adds it to the
     */
    private synchronized void consumeTick() {

        while (tickArrivals.size() == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Ticker aPlainTick =  tickArrivals.pollFirst();
        //PriceEqualTick aPlainTick = (PriceEqualTick) tickArrivals.pollFirst();
        TreeSet<Ticker> tickerQ = tickerMap.get(aPlainTick.getInstrument());

        synchronized (lock) {
            //addToSortedSet(tickerQ, , aPlainTick);
            addToSortedSet(tickerQ, (a, b) -> {
                return (int) (a.getTimestamp() - b.getTimestamp());
            }, aPlainTick);

            addTickToPQ(tickerMinPQMap, (a, b) -> {
                return (int) (a.getPrice() - b.getPrice());
            }, aPlainTick);
            addTickToPQ(tickerMaxPQMap, (a, b) -> {
                return (int) (b.getPrice() - a.getPrice());
            }, aPlainTick);

            addToTotalPrice(aPlainTick);
            addToTotalCount(aPlainTick);
        }

        synchronized (stats_lock) {
            addToTickerStatus(aPlainTick);
        }


    }

    private void removeFromTickerStatus(Ticker aPlainTick) {

        TreeSet<Ticker> tickerQ = tickerMap.get(aPlainTick.getInstrument());

        if (tickerQ == null) {
            tickerStatsMap.remove(aPlainTick.getInstrument());
            return;
        }

        updateStats(aPlainTick, tickerStatsMap.get(aPlainTick.getInstrument()));
    }


    private void addToTickerStatus(Ticker aPlainTick) {
        TickStats ts = tickerStatsMap.get(aPlainTick.getInstrument());

        if (ts == null) {
            ts = new TickStats(aPlainTick.getInstrument(),0.0,0.0,0.0,0L);
        }

        updateStats(aPlainTick, ts);
    }

    private void updateStats(Ticker aPlainTick, TickStats ts) {
        ts.setCount(tickerTotalCountMap.get(aPlainTick.getInstrument()));
        ts.setMin(getMin(aPlainTick.getInstrument(), aPlainTick.getPrice()));
        ts.setMax(getMax(aPlainTick.getInstrument(), aPlainTick.getPrice()));
        ts.setAvg(getAvg(aPlainTick.getInstrument()));
        tickerStatsMap.put(aPlainTick.getInstrument(),ts);
    }


    private Double getMin(String anInstrument, Double aDefaultVal ){

        Double ret = aDefaultVal;
        PriorityQueue<Ticker> pq = tickerMinPQMap.get(anInstrument);

        if (pq != null) {
            Ticker t = pq.peek();
            if (t != null) {
                ret = t.getPrice();
            }
        }

        return ret;
    }

    private Double getMax(String anInstrument, Double aDefaultVal){

        Double ret = aDefaultVal;

        Optional<PriorityQueue<Ticker>> pq = Optional.ofNullable(tickerMaxPQMap.get(anInstrument));
        if (pq.isPresent()) {
            return pq.get().peek().getPrice();
        }
        else {
            return aDefaultVal;
        }

    }

    public Double getAvg(String anInstrument ){

        Optional<Double> optval = Optional.ofNullable(tickerTotalPriceMap.get(anInstrument));

        Double ret = null;
        if (optval.isPresent()) {
            ret = optval.get() / Optional.ofNullable(tickerTotalCountMap.get(anInstrument)).orElse(1L);
        }

        return ret;
    }


    /**
     * Adds the price of instrument into the total
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param aPlainTick
     */
    private void addToTotalPrice(Ticker aPlainTick) {

        double optval = Optional.ofNullable(tickerTotalPriceMap.get(aPlainTick.getInstrument())).orElse(0.0);

        tickerTotalPriceMap.put(aPlainTick.getInstrument(), optval + aPlainTick.getPrice());
    }

    /**
     * Subtracts the price of instrument from the total
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param aPlainTick
     */
    private void removeFromTotalPrice(Ticker aPlainTick) {

        Double val = tickerTotalPriceMap.get(aPlainTick.getInstrument());

        if (val != null) {
            tickerTotalPriceMap.put(aPlainTick.getInstrument(), val - aPlainTick.getPrice());
        }
    }

    /**
     * Increments total count of instrument in the window
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param aPlainTick
     */
    private void addToTotalCount(Ticker aPlainTick) {

        long optval = Optional.ofNullable(tickerTotalCountMap.get(aPlainTick.getInstrument())).orElse(0L);

        tickerTotalCountMap.put(aPlainTick.getInstrument(), optval + 1);
    }

    /**
     * Decrements total count of instrument in the window
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param aPlainTick
     */
    private void removeFromTotalCount(Ticker aPlainTick) {

        Long val = tickerTotalCountMap.get(aPlainTick.getInstrument());

        if (val != null) {
            tickerTotalCountMap.put(aPlainTick.getInstrument(), val - 1L);
        }
    }

    /**
     * Updates the statistics of instrument that's leaving the window
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param aPlainTick
     */
    private TreeSet<Ticker> remove(TreeSet<Ticker> tickerQ, Ticker aPlainTick) {

        TreeSet<Ticker> ret = null;

        synchronized (lock) {
            ret = removeFromQueue(tickerQ, aPlainTick.getInstrument());
            if (ret != null) {
                removeTickFromPQ(tickerMinPQMap,  aPlainTick);
                removeTickFromPQ(tickerMaxPQMap,  aPlainTick);

                removeFromTotalPrice(aPlainTick);
                removeFromTotalCount(aPlainTick);
            }
            else {
                tickerMinPQMap.remove(aPlainTick.getInstrument());
                tickerMaxPQMap.remove(aPlainTick.getInstrument());
                tickerTotalPriceMap.remove(aPlainTick.getInstrument());
                tickerTotalCountMap.remove(aPlainTick.getInstrument());
            }

        }

        synchronized (stats_lock) {
            removeFromTickerStatus(aPlainTick);
        }

        return ret;
    }


    /**
     * Updates the min/max price of the instrument that's entering the window
     * Time Complexity O(log N)  Due to the rebalancing of he priority queue on insertion of new value
     * Space Complexity O(1)
     * @param aPQMap  min or max PQ
     * @param aComp  min or max Comparator
     * @param aPlainTick
     */
    private void addTickToPQ(Map<String, PriorityQueue<Ticker>> aPQMap, Comparator<Ticker> aComp, Ticker aPlainTick) {

        PriorityQueue<Ticker> pq = aPQMap.get(aPlainTick.getInstrument());

        if (pq == null) {
            pq = new PriorityQueue<>((a, b)-> { return aComp.compare(a,b);});
        }

        pq.offer((aPlainTick));
        aPQMap.put(aPlainTick.getInstrument(),pq);
    }

    /**
     * Updates the min/max price of the instrument that's leaving the window
     * Time Complexity O(log N)  Due to the rebalancing of he priority queue on removal of value
     * Space Complexity O(1)
     * @param aPQMap  min or max PQ
     * @param aPlainTick
     */
    private void removeTickFromPQ(Map<String, PriorityQueue<Ticker>> aPQMap, Ticker aPlainTick) {

        PriorityQueue<Ticker> pq = aPQMap.get(aPlainTick.getInstrument());

        if (pq == null) {
            return;
        }
        else {
            pq.remove(aPlainTick);
            if (pq.isEmpty()) {
                aPQMap.remove(aPlainTick.getInstrument());
            }
            else {
                aPQMap.put(aPlainTick.getInstrument(), pq);
            }
        }
    }


    /**
     * Adds the instrument that's entering the window into the SortedSet acting as a Queue
     * Time Complexity O(1)
     * Space Complexity O(1)
     *
     * @param tickerQ
     * @param aComp
     * @param aPlainTick
     */
    private void addToSortedSet(TreeSet<Ticker> tickerQ, Comparator<Ticker> aComp, Ticker aPlainTick) {

        if (tickerQ == null) {
            tickerQ = new TreeSet<>(aComp);
        }

        tickerQ.add(aPlainTick);
        tickerMap.put(aPlainTick.getInstrument(),tickerQ);

    }


    /**
     * Removes the instrument that's leaving the window from the head of the queue
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param tickerQ
     * @param anInstrument
     * @return tickerQ
     */
    private TreeSet<Ticker> removeFromQueue(TreeSet<Ticker> tickerQ, String anInstrument) {

        if (tickerQ == null) {
            return null;
        }

        tickerQ.pollFirst();

        if(tickerQ.isEmpty()) {
            tickerMap.remove(anInstrument);
            tickerQ = null;
        }
        else {
            tickerMap.put(anInstrument, tickerQ);
        }

        return tickerQ;
    }


    /**
     * Moves the sliding window of time on the queue of elements ordered by the timestamps
     * Examines the tick at the head of the Queue that's kept for each instrument in a loop,
     * and checks whether  it is still within the predefined sliding time window, removing it if it's not anymore.
     *  It removes it
     *
     */
    void prune() {

        Temporal now = ZonedDateTime.now() ;

        for(TreeSet<Ticker> tickerQ: tickerMap.values()) {
                Ticker t = tickerQ.first();

                while (t != null) {
                    Instant tickts = Instant.ofEpochMilli(t.getTimestamp());
                    if (! is_within_window(tickts, now, windowMillis)) {
                        tickerQ = remove(tickerQ, t);
                        if (tickerQ != null) {
                            t = tickerQ.first();
                        }
                    } else {
                        break;
                    }
                } //while
        }//for

    }

    public static boolean is_within_window(Instant tickTs, Temporal now, long windowMillis) {

        Duration dur = Duration.between(tickTs, now);
        boolean ret = (dur.getSeconds() * MSECS_PER_SECOND > windowMillis ? false : true);

        return ret;

    }

    public Map<String, TickStats> getStats() {

        Map<String, TickStats> ret = null;

        synchronized (stats_lock) {
             ret = tickerStatsMap;
        }

        return ret;
    }

    @Override
    public void run() {

        while(isRunning) {
            consumeTick();
        }

        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
