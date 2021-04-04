package com.cuyan.indexer.util;

import com.cuyan.indexer.model.Tick;
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

    private Deque<Tick> tickArrivals = null;

    private Map<String, TreeSet<Tick>> tickerMap = null;
    private Map<String, TickStats> tickerStatsMap = null;
    private Map<String, PriorityQueue<Tick>> tickerMinPQMap;
    private Map<String, PriorityQueue<Tick>> tickerMaxPQMap;
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
    public synchronized void add(Tick aPlainTick) {

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

        tickerMap = new HashMap<String, TreeSet<Tick>>();
        tickerStatsMap = new HashMap<>();
        tickerMinPQMap = new HashMap<String, PriorityQueue<Tick>>();
        tickerMaxPQMap = new HashMap<String, PriorityQueue<Tick>>();
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

        Tick aPlainTick =  tickArrivals.pollFirst();
        //PriceEqualTick aPlainTick = (PriceEqualTick) tickArrivals.pollFirst();
        TreeSet<Tick> tickQ = tickerMap.get(aPlainTick.getInstrument());

        synchronized (lock) {
            //addToSortedSet(tickQ, , aPlainTick);
            addToSortedSet(tickQ, (a, b) -> {
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

    private void removeFromTickerStatus(Tick aPlainTick) {

        TreeSet<Tick> tickQ = tickerMap.get(aPlainTick.getInstrument());

        if (tickQ == null) {
            tickerStatsMap.remove(aPlainTick.getInstrument());
            return;
        }

        updateStats(aPlainTick, tickerStatsMap.get(aPlainTick.getInstrument()));
    }


    private void addToTickerStatus(Tick aPlainTick) {
        TickStats ts = tickerStatsMap.get(aPlainTick.getInstrument());

        if (ts == null) {
            ts = new TickStats(aPlainTick.getInstrument(),0.0,0.0,0.0,0L);
        }

        updateStats(aPlainTick, ts);
    }

    private void updateStats(Tick aPlainTick, TickStats ts) {
        ts.setCount(tickerTotalCountMap.get(aPlainTick.getInstrument()));
        ts.setMin(getMin(aPlainTick.getInstrument(), aPlainTick.getPrice()));
        ts.setMax(getMax(aPlainTick.getInstrument(), aPlainTick.getPrice()));
        ts.setAvg(getAvg(aPlainTick.getInstrument()));
        tickerStatsMap.put(aPlainTick.getInstrument(),ts);
    }


    private Double getMin(String anInstrument, Double aDefaultVal ){

        Double ret = aDefaultVal;
        PriorityQueue<Tick> pq = tickerMinPQMap.get(anInstrument);

        if (pq != null) {
            Tick t = pq.peek();
            if (t != null) {
                ret = t.getPrice();
            }
        }

        return ret;
    }

    private Double getMax(String anInstrument, Double aDefaultVal){

        Double ret = aDefaultVal;

        Optional<PriorityQueue<Tick>> pq = Optional.ofNullable(tickerMaxPQMap.get(anInstrument));
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
    private void addToTotalPrice(Tick aPlainTick) {

        double optval = Optional.ofNullable(tickerTotalPriceMap.get(aPlainTick.getInstrument())).orElse(0.0);

        tickerTotalPriceMap.put(aPlainTick.getInstrument(), optval + aPlainTick.getPrice());
    }

    /**
     * Subtracts the price of instrument from the total
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param aPlainTick
     */
    private void removeFromTotalPrice(Tick aPlainTick) {

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
    private void addToTotalCount(Tick aPlainTick) {

        long optval = Optional.ofNullable(tickerTotalCountMap.get(aPlainTick.getInstrument())).orElse(0L);

        tickerTotalCountMap.put(aPlainTick.getInstrument(), optval + 1);
    }

    /**
     * Decrements total count of instrument in the window
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param aPlainTick
     */
    private void removeFromTotalCount(Tick aPlainTick) {

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
    private TreeSet<Tick> remove(TreeSet<Tick> tickQ, Tick aPlainTick) {

        TreeSet<Tick> ret = null;

        synchronized (lock) {
            ret = removeFromQueue(tickQ, aPlainTick.getInstrument());
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
    private void addTickToPQ(Map<String, PriorityQueue<Tick>> aPQMap, Comparator<Tick> aComp, Tick aPlainTick) {

        PriorityQueue<Tick> pq = aPQMap.get(aPlainTick.getInstrument());

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
    private void removeTickFromPQ(Map<String, PriorityQueue<Tick>> aPQMap, Tick aPlainTick) {

        PriorityQueue<Tick> pq = aPQMap.get(aPlainTick.getInstrument());

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
     * @param tickQ
     * @param aComp
     * @param aPlainTick
     */
    private void addToSortedSet(TreeSet<Tick> tickQ, Comparator<Tick> aComp, Tick aPlainTick) {

        if (tickQ == null) {
            tickQ = new TreeSet<>(aComp);
        }

        tickQ.add(aPlainTick);
        tickerMap.put(aPlainTick.getInstrument(), tickQ);

    }


    /**
     * Removes the instrument that's leaving the window from the head of the queue
     * Time Complexity O(1)
     * Space Complexity O(1)
     * @param tickQ
     * @param anInstrument
     * @return tickQ
     */
    private TreeSet<Tick> removeFromQueue(TreeSet<Tick> tickQ, String anInstrument) {

        if (tickQ == null) {
            return null;
        }

        tickQ.pollFirst();

        if(tickQ.isEmpty()) {
            tickerMap.remove(anInstrument);
            tickQ = null;
        }
        else {
            tickerMap.put(anInstrument, tickQ);
        }

        return tickQ;
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

        for(TreeSet<Tick> tickQ : tickerMap.values()) {
                Tick t = tickQ.first();

                while (t != null) {
                    Instant tickts = Instant.ofEpochMilli(t.getTimestamp());
                    if (! is_within_window(tickts, now, windowMillis)) {
                        tickQ = remove(tickQ, t);
                        if (tickQ != null) {
                            t = tickQ.first();
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
