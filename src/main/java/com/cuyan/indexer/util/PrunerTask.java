package com.cuyan.indexer.util;

import java.util.TimerTask;

public class PrunerTask extends TimerTask {

    private SlidingWindow sw=null;

    public PrunerTask(SlidingWindow slidingWindow) {
        sw = slidingWindow;
    }

    @Override
    public void run() {
        sw.prune();
    }
}
