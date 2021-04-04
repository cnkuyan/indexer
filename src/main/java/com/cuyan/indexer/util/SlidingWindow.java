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

    private int arrivalBufferSize = 0;
    private long windowMillis = 0;
    private long timerRefreshMSecs = 0;
    private boolean isRunning = false;

    private Deque<Tick> tickArrivals = null;

    private Map<String, PriorityQueue<Tick>> tickerMap = null;
    private Map<String, TickStats> tickerStatsMap = null;
    private Map<String, PriorityQueue<Tick>> tickerMinPQMap;
    private Map<String, PriorityQueue<Tick>> tickerMaxPQMap;
    private Map<String, Double> tickerTotalPriceMap;
    private Map<String, Long> tickerTotalCountMap;

    private Object lock = new Object();
    private Object stats_lock = new Object();
    private Thread t = null;
    private Timer pruneTimer = null;
    private Comparator<Tick> timeStampComparator = null;
    private Comparator<Tick> minPriceComparator = null;
    private Comparator<Tick> maxPriceComparator = null;


    /**
     * Initializes the sliding time window
     * Also initializes the following Comparators;
     *
     * The comparator that's used to decide how to order an incoming @see Tick
     * (by the order of the timestamp field, larger number first - which means an older Tick )
     *
     * The comparator that's used to order Ticks by their prices in ascending order
     * The comparator that's used to order Ticks by their prices in descending order
     *
     * @param bufferSize  the size of the blocking buffer the incoming ticks are queued in the order they are added
     * @param aWindowMsecs the duration of the sliding time windows in milliseconds
     * @param aTimerRefreshMSecs the duration of the period the periodic pruner task runs
     */
    public SlidingWindow(int bufferSize, long aWindowMsecs, long aTimerRefreshMSecs) {
        arrivalBufferSize = bufferSize;
        windowMillis = aWindowMsecs;
        timerRefreshMSecs= aTimerRefreshMSecs;
        tickArrivals = new ArrayDeque<>(bufferSize);

        timeStampComparator = (a, b) -> {
            return (int) (a.getTimestamp() - b.getTimestamp());
        };

        minPriceComparator = (a, b) -> {
            return (int) (a.getPrice() - b.getPrice());
        };

        maxPriceComparator = (a, b) -> {
            return (int) (b.getPrice() - a.getPrice());
        };

        initMaps();

    }

    /**
     * Starts the thread that waits on the ticksarrival queye to consume from it
     * Also starts the timer that periodically runs the prune task that retires the
     * ticks that fall behind the sliding time window of duration aWindowMsecs
     * @see SlidingWindow
     */
    public void start() {

        t = new Thread(this);
        t.start();
        isRunning = true;
        logger.debug("SW Thread started!");
        startTimer(timerRefreshMSecs);
    }

    /**
     * Stops the consuming thread, and the prune periodic task
     */
    public void stop() {
        logger.debug("SlidingWindow stop() called!");

        isRunning = false;
        pruneTimer.cancel();
    }

    /**
     * Called by the @see TickController to add an incoming Tick in the order it was received.
     * If there is still space in the intermediate buffer defined by arrivalBufferSize ,
     * the Tick will be added at the end, and the consumer thread will be notified.
     *
     * The call will only block only when the intermediate buffer is full.
     *
     * This case may happen if there are more incoming Ticks than the consumer thread alone can consume
     * from the tickArrivals
     */
    public synchronized void add(Tick aPlainTick) {

        while (tickArrivals.size() == arrivalBufferSize)  {
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
     * Initializes the map data structures used throughout
     */
    private void initMaps() {

        tickerMap = new HashMap<String, PriorityQueue<Tick>>();
        tickerStatsMap = new HashMap<>();
        tickerMinPQMap = new HashMap<String, PriorityQueue<Tick>>();
        tickerMaxPQMap = new HashMap<String, PriorityQueue<Tick>>();
        tickerTotalPriceMap = new HashMap<>();
        tickerTotalCountMap = new HashMap<>();
    }

    /**
     * Starts the periodic prune task
     * @param aPeriod duration of the period in milliseconds
     */
    private void startTimer(long aPeriod) {

        pruneTimer = new Timer();
        PrunerTask st = new PrunerTask(this);
        pruneTimer.schedule(st, 0, aPeriod);
    }



    /**
     * Consumes a Tick from the head of the tickArrivals queue upon waking
     * blocks and waits for notification if the queue is empty
     *
     * Upon picking the next Tick in the order it came, it adds it to the sliding window queue
     * which is backed by a PriorityQueue, initialized with the timeStampComparator .
     * The sliding window PQ always gives the oldest Tick (based on its timestamp field) when as its top element.
     *
     * It also adds the incoming Tick to the appropriate PQs , to calculate the min and max price,
     * and to update the average price and the count of the instrument identified by the Tick
     * within the valid time window.
     *
     * It also updates the TickerStatus map that's used to provide the real time statistics of all instruments
     * currently active within the defined sliding window of time.
     *
     * Time Complexity: O(logN)
     * Space Complexity O(N)
     *
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
        PriorityQueue<Tick> tickQ = tickerMap.get(aPlainTick.getInstrument());

        synchronized (lock) {
            addToQueue(tickQ, timeStampComparator, aPlainTick);

            addTickToPQ(tickerMinPQMap, minPriceComparator, aPlainTick);
            addTickToPQ(tickerMaxPQMap, maxPriceComparator, aPlainTick);

            addToTotalPrice(aPlainTick);
            addToTotalCount(aPlainTick);
        }

        synchronized (stats_lock) {
            addToTickerStats(aPlainTick);
        }


    }

    /**
     * Updates the TickerStatus map for a Tick that's being removed from the sliding time window
     * If there are no more relevant Tick is left in the sliding time window, it removes the key from the map
     * completely
     *
     * @param aPlainTick
     */
    private void removeFromTickerStats(Tick aPlainTick) {

        PriorityQueue<Tick> tickQ = tickerMap.get(aPlainTick.getInstrument());

        if (tickQ == null) {
            tickerStatsMap.remove(aPlainTick.getInstrument());
            return;
        }

        updateStats(aPlainTick, tickerStatsMap.get(aPlainTick.getInstrument()));
    }


    /**
     * Updates the TickerStatus map for a Tick that's being introduced to the sliding time window
     * If this is first time this instrument is seen, a new key gets created and inserted in to the map
     *
     * @param aPlainTick
     */
    private void addToTickerStats(Tick aPlainTick) {
        TickStats ts = tickerStatsMap.get(aPlainTick.getInstrument());

        if (ts == null) {
            ts = new TickStats(aPlainTick.getInstrument(),0.0,0.0,0.0,0L);
        }

        updateStats(aPlainTick, ts);
    }

    /**
     * Updates the TickStats map according to the given Tick
     * Time Complexity: O(1)
     * Space Complexity O(N)
     *
     * @param aPlainTick
     * @param ts
     */
    private void updateStats(Tick aPlainTick, TickStats ts) {
        ts.setCount(tickerTotalCountMap.get(aPlainTick.getInstrument()));
        ts.setMin(getMin(aPlainTick.getInstrument(), aPlainTick.getPrice()));
        ts.setMax(getMax(aPlainTick.getInstrument(), aPlainTick.getPrice()));
        ts.setAvg(getAvg(aPlainTick.getInstrument()));
        tickerStatsMap.put(aPlainTick.getInstrument(),ts);
    }


    /**
     * Calculates the minimum price of the given instrument within the current sliding time window
     * Time Complexity: O(1)
     * Space Complexity O(N)
     * @param anInstrument
     * @param aDefaultVal
     * @return the min price
     */
    private Double getMin(String anInstrument, Double aDefaultVal ){

        Double ret = aDefaultVal;

        Optional<PriorityQueue<Tick>> pq = Optional.ofNullable(tickerMinPQMap.get(anInstrument));
        if (pq.isPresent()) {
            return pq.get().peek().getPrice();
        }
        else {
            return aDefaultVal;
        }

    }

    /**
     * Calculates the maximum price of the given instrument within the current sliding time window
     * Time Complexity: O(1)
     * Space Complexity O(N)
     *
     * @param anInstrument
     * @param aDefaultVal
     * @return the max price
     */

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

    /**
     * Calculates the average price of the given instrument within the current sliding time window
     * Time Complexity: O(1)
     * Space Complexity O(1)
     * @param anInstrument
     * @return the average price
     */
    public Double getAvg(String anInstrument ){

        Optional<Double> optval = Optional.ofNullable(tickerTotalPriceMap.get(anInstrument));

        Double ret = null;
        if (optval.isPresent()) {
            ret = optval.get() / Optional.ofNullable(tickerTotalCountMap.get(anInstrument)).orElse(1L);
        }

        return ret;
    }


    /**
     * Adds the price of the given instrument into the running total
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
    private void subtractFromTotalPrice(Tick aPlainTick) {

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
    private void subtractFromTotalCount(Tick aPlainTick) {

        Long val = tickerTotalCountMap.get(aPlainTick.getInstrument());

        if (val != null) {
            tickerTotalCountMap.put(aPlainTick.getInstrument(), val - 1L);
        }
    }

    /**
     * Updates the statistics of instrument that's leaving the window
     * Time Complexity O(1)
     * Space Complexity O(N)
     * @param aPlainTick
     */
    private PriorityQueue<Tick> remove(PriorityQueue<Tick> tickQ, Tick aPlainTick) {

        PriorityQueue<Tick> ret = null;

        synchronized (lock) {
            ret = removeFromQueue(tickQ, aPlainTick.getInstrument());
            if (ret != null) {
                removeTickFromPQ(tickerMinPQMap,  aPlainTick);
                removeTickFromPQ(tickerMaxPQMap,  aPlainTick);

                subtractFromTotalPrice(aPlainTick);
                subtractFromTotalCount(aPlainTick);
            }
            else {
                tickerMinPQMap.remove(aPlainTick.getInstrument());
                tickerMaxPQMap.remove(aPlainTick.getInstrument());
                tickerTotalPriceMap.remove(aPlainTick.getInstrument());
                tickerTotalCountMap.remove(aPlainTick.getInstrument());
            }

        }

        synchronized (stats_lock) {
            removeFromTickerStats(aPlainTick);
        }

        return ret;
    }


    /**
     * Updates the min/max price of the instrument that's entering the window
     * Time Complexity O(log N)  due to the rebalancing of he priority queue on insertion of new value
     * Space Complexity O(N)
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
     * Time Complexity O(log N)  due to the rebalancing of he priority queue on removal of value
     * Space Complexity O(N)
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
     * Adds the Tick that's entering the window into the queue (PriorityQueue)
     * Time Complexity O(log N)  due to the rebalancing of the priority queue on value insertion
     * Space Complexity O(N)
     *
     * @param tickQ the target queue
     * @param aComp the comparator that places an older timestamped Tick at the front of the queue
     * @param aPlainTick a Tick to enqueue
     */
    private void addToQueue(PriorityQueue<Tick> tickQ, Comparator<Tick> aComp, Tick aPlainTick) {

        if (tickQ == null) {
            tickQ = new PriorityQueue<>(aComp);
        }

        tickQ.offer(aPlainTick);
        tickerMap.put(aPlainTick.getInstrument(), tickQ);

    }


    /**
     * Removes the instrument that's leaving the window from the head of the queue
     * Time Complexity O(log N)  due to the rebalancing of the priority queue on value removal
     * Space Complexity O(N)
     * @param tickQ
     * @param anInstrument
     * @return tickQ
     */
    private PriorityQueue<Tick> removeFromQueue(PriorityQueue<Tick> tickQ, String anInstrument) {

        if (tickQ == null) {
            return null;
        }

        Tick polled = tickQ.poll();
        logger.info("removeFromQueue: removed [%s]" + polled);

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

        for(PriorityQueue<Tick> tickQ : tickerMap.values()) {
                Tick t = tickQ.peek();

                while (t != null) {
                    Instant tickts = Instant.ofEpochMilli(t.getTimestamp());
                    if (! is_within_window(tickts, now, windowMillis)) {
                        tickQ = remove(tickQ, t);
                        if (tickQ != null) {
                            t = tickQ.peek();
                        }
                    } else {
                        break;
                    }
                } //while
        }//for

    }

    /**
     * Decides whether the given Tick is still relevant , given the current time and a time window
     * @param tickTs  Tick's timestamp
     * @param now  current time
     * @param windowMillis duration of time window in milliseconds
     * @return  true if the Tick's timestamp is within the specified window of time based on server's current time
     */
    public static boolean is_within_window(Instant tickTs, Temporal now, long windowMillis) {

        Duration dur = Duration.between(tickTs, now);
        boolean ret = (dur.getSeconds() * MSECS_PER_SECOND > windowMillis ? false : true);

        return ret;

    }

    /**
     *  Called by the TickController to obtain the current snapshot the TickStats ( statistics)
     *
     *  The returned map will have keys only for those instruments that are still relevant within the
     *  current sliding time window.
     *
     * @return TickStats map
     */
    public Map<String, TickStats> getStats() {

        Map<String, TickStats> ret = null;

        synchronized (stats_lock) {
             ret = tickerStatsMap;
        }

        return ret;
    }

    /**
     * The worker method of the thread consuming from the ticksArrival dequeu
     */
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
