package com.cuyan.indexer.service;

import com.cuyan.indexer.model.Tick;
import com.cuyan.indexer.model.TickStats;
import com.cuyan.indexer.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.cuyan.indexer.IndexerApplication.*;

@Service
public class TickService {

    Logger log = LoggerFactory.getLogger(TickService.class);

    private  long windowMsecs = 0;
    private  Map<String, TickStats> statsMap = null;
    private SlidingWindow slidingWindow = null;


    /**
     *
     */
    public TickService() {
        init(MAX_QUEUE_SIZE,SLIDINGWINDOW_DURATION_SECS * MSECS_PER_SECOND,TIMER_REFRESH_MSECS);
    }

    /**
     *
     * @param aSize
     * @param aWindowMsecs
     * @param aTimerRefreshPeriodMsecs
     */
    public TickService(int aSize, long aWindowMsecs, long aTimerRefreshPeriodMsecs) {
        init(aSize, aWindowMsecs, aTimerRefreshPeriodMsecs);
    }

    /**
     *
     */
    private void init(int aSize, long aWindowMsecs, long aTimerRefreshPeriodMsecs){
        windowMsecs = aWindowMsecs;
        statsMap = new ConcurrentHashMap<>();
        slidingWindow = new SlidingWindow(aSize,windowMsecs,aTimerRefreshPeriodMsecs);
        slidingWindow.start();

    }

    @PreDestroy
    public void stop() {
        slidingWindow.stop();
    }

    /**
     * Called by the TickController to register a Tick in the order it is received.
     *
     * it checks the timestamp field of the given Tick, and accepts it only if it's still in the
     * last windowMsecs based on server's  current datetime
     *
     * It accepts it if it is, rejects it otherwise
     *
     * @param aTick
     * @return  HttpStatus.NO_CONTENT if the Tick is older than the tail of the sliding time window
     *          HttpStatus.OK  if the Tick is accepted
     */
    public ResponseEntity add(Tick aTick) {

        Instant tickts = Instant.ofEpochMilli(aTick.getTimestamp());
        if(! SlidingWindow.is_within_window(tickts,ZonedDateTime.now(), windowMsecs)) {
            log.error("add: HttpStatus.NO_CONTENT");
            return new ResponseEntity<>(null, HttpStatus.NO_CONTENT);
        } else {
            slidingWindow.add(aTick);
            log.debug("add: HttpStatus.OK");
            return new ResponseEntity<>(null, HttpStatus.OK);
        }

    }

    /**
     * Called by the TickController to refresh the snapshot of the TickStats map
     * @return
     */
    private Map<String, TickStats> refreshMap() {

        statsMap = slidingWindow.getStats();
        return statsMap;

    }

    /**
     * Called by the TickController to obtain the snapshot of the TickStats map
     * containing all relevant Instruments
     *
     * @return
     */
    public Map<String, TickStats> stats() {
        Map<String, TickStats> ret = refreshMap();
        return ret;
    }

    /**
     * Called by the TickController to obtain the snapshot of the TickStats instance
     * belonging to the given instrument
     *
     * @param instrument
     * @return
     */
    public TickStats stats(String instrument) {
        TickStats ret = refreshMap().get(instrument);
        return ret;
    }
}
