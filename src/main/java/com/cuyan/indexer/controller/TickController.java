package com.cuyan.indexer.controller;

import com.cuyan.indexer.model.Tick;
import com.cuyan.indexer.model.TickStats;
import com.cuyan.indexer.service.TickService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
public class TickController {

    Logger log = LoggerFactory.getLogger(TickController.class);

    @Autowired
    private final TickService tickService = null;


    @PostMapping(value = "/ticks",consumes = {"application/json"},produces = {"application/json"})
     public ResponseEntity registerTick(@RequestBody Tick plainTick) {
        ResponseEntity ret = tickService.add(plainTick);
        return ret;
    }

    @GetMapping("/statistics")
    public ResponseEntity stats() {
        Map<String, TickStats> retmap = tickService.stats();
        List<TickStats>  values = null;
        if (retmap != null) {
            values = new ArrayList<>(retmap.values());
        }
        return ResponseEntity.of(Optional.ofNullable(values));
    }

    @GetMapping("/statistics/{instrument}")
    public ResponseEntity<TickStats> stats_for_tick(@PathVariable String instrument) {
        TickStats ret = tickService.stats(instrument);
        return ResponseEntity.of(Optional.ofNullable(ret));
    }

}
