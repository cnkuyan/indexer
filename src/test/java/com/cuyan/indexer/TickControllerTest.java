package com.cuyan.indexer;

import com.cuyan.indexer.model.Tick;
import com.cuyan.indexer.model.TickStats;
import com.cuyan.indexer.service.TickService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static com.cuyan.indexer.IndexerApplication.MSECS_PER_SECOND;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
@WebMvcTest
public class TickControllerTest {

    private final static String STATS_URL = "/statistics/";
    private final static String TICKS_URL = "/ticks";
    private Map<String, TickStats> tickStatsMap = buildTickStatsMap();

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TickService tickService;

    @Test
    void postTick() throws Exception {

        // when
        Tick aPriceEqualTick = makeValidTick();
        when(tickService.add(aPriceEqualTick)).thenReturn(new ResponseEntity<>(null, HttpStatus.OK));


        ObjectMapper om = new ObjectMapper();
        String aTickJSON = om.writeValueAsString(aPriceEqualTick);

        mockMvc.perform(post(TICKS_URL)
                .contentType(MediaType.APPLICATION_JSON)
                .content(aTickJSON))
                .andExpect(status().isOk());

    }


    @Test
    void getSingleTickStats() throws Exception {

        // when
        when(tickService.stats("ABC")).thenReturn(tickStatsMap.get("ABC"));


        String resultJson = "{\"instrument\":\"ABC\",\"min\":93.4,\"max\":110.3,\"count\":46}";

        mockMvc.perform(get(STATS_URL+"ABC"))
                .andExpect(status().isOk())
                .andExpect(content().json(resultJson));

    }

    @Test
    void getAllStats() throws Exception {

        // when
        when(tickService.stats()).thenReturn(tickStatsMap);


        ObjectMapper om = new ObjectMapper();
        String resultJson = om.writeValueAsString(tickStatsMap.values());

        mockMvc.perform(get(STATS_URL))
                .andExpect(status().isOk())
                .andExpect(content().json(resultJson));

    }

    private Map<String, TickStats> buildTickStatsMap() {

        TickStats tick1 = new TickStats("XYZ",10.0,11.2,10.5,34);
        TickStats tick2 = new TickStats("ABC",93.4,110.3,98.0, 46);

        Map<String,TickStats> map = new HashMap<>(2);
        map.put("XYZ",tick1);
        map.put("ABC",tick2);

        return map;
    }

    private Tick makeValidTick() {

        ZonedDateTime start_time = ZonedDateTime.now();
        long epoch_msecs_till_now = start_time.toEpochSecond() * MSECS_PER_SECOND;

        return  new Tick("XYZ",10.0,epoch_msecs_till_now );

    }


}
