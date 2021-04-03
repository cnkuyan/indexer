package com.cuyan.indexer.service;

import com.cuyan.indexer.model.Ticker;
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

    private  Map<String, TickStats> statsMap = null;
    private  long windowMsecs = 0;
    private SlidingWindow slidingWindow = null;


    //@Autowired
    //private SlidingWindowService swService = null;

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
        //startSWService();
        slidingWindow = new SlidingWindow(aSize,windowMsecs,aTimerRefreshPeriodMsecs);
        slidingWindow.start();

    }

    public void start() {
        slidingWindow.start();
    }

    @PreDestroy
    public void stop() {
        slidingWindow.stop();
    }

    /**
     *
     * @param aPlainTick
     * @return
     */
    public ResponseEntity add(Ticker aPlainTick) {

        Instant tickts = Instant.ofEpochMilli(aPlainTick.getTimestamp());
        if(! SlidingWindow.is_within_window(tickts,ZonedDateTime.now(), windowMsecs)) {
            log.error("add: HttpStatus.NO_CONTENT");
            return new ResponseEntity<>(null, HttpStatus.NO_CONTENT);
        } else {
            slidingWindow.add(aPlainTick);
            log.info("add: HttpStatus.OK");
            return new ResponseEntity<>(null, HttpStatus.OK);
        }

    }

    private Map<String, TickStats> refreshMap() {

        statsMap = slidingWindow.getStats();
        return statsMap;

    }

    public Map<String, TickStats> stats() {
        //final ReadOnlyKeyValueStore<String, TickStats> store =interactiveQueryService.getQueryableStore("tick-stats", QueryableStoreTypes.<String, TickStats>keyValueStore());
        Map<String, TickStats> ret = refreshMap();
        return ret;
    }

    public TickStats stats(String instrument) {
        TickStats ret = refreshMap().get(instrument);
        return ret;
    }
}
