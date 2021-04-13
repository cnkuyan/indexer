package com.cuyan.indexer;

import com.cuyan.indexer.model.Tick;
import com.cuyan.indexer.model.TickStats;
import com.cuyan.indexer.service.TickService;
import com.cuyan.indexer.util.SlidingWindow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.cuyan.indexer.IndexerApplication.MSECS_PER_SECOND;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class SlidingWindowTest {
	private static final long SLIDINGWINDOW_DURATION_SECS_TEST = 4;


	@BeforeEach
	public void setup() {
	}

	@AfterEach
	public void shutdown() {

	}

	@Test
	public void test() throws InterruptedException {

		final int TICK_CNT = 5;
		SlidingWindow sw = new SlidingWindow(64, (TICK_CNT+2) * MSECS_PER_SECOND, 1 * MSECS_PER_SECOND);

		String instrument = "XYZ";
		ZonedDateTime start_time = ZonedDateTime.now();
		long epoch_now = start_time.toEpochSecond() * MSECS_PER_SECOND;

		List<Tick> ticks = new ArrayList<>(TICK_CNT);

		//Create ticks , younger to older
		ticks.add(new Tick(instrument,10,epoch_now - (long)( (1) * MSECS_PER_SECOND)));
		ticks.add(new Tick(instrument,20,epoch_now - (long)( (2) * MSECS_PER_SECOND)));
		ticks.add(new Tick(instrument,30,epoch_now - (long)( (3) * MSECS_PER_SECOND)));
		ticks.add(new Tick(instrument,40,epoch_now - (long)( (4) * MSECS_PER_SECOND)));
		ticks.add(new Tick(instrument,50,epoch_now - (long)( (5) * MSECS_PER_SECOND)));


		//Add ticks to the slidingwindow, young first
		double total=0,avg = 0.0;
		for (Tick t: ticks) {
			sw.add(t);
			total += t.getPrice();
		}
		avg = total / TICK_CNT;

		//Start the slidingwindow processing
		//Expect: old ticks consumed first
		sw.start();

		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		Map<String, TickStats> sm = sw.getStats();

		assertEquals(TICK_CNT,sm.get(instrument).getCount());
		assertEquals(10.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(avg,sm.get(instrument).getAvg());

		Thread.sleep((long) 2.5 * MSECS_PER_SECOND);

		sm = sw.getStats();
		assertEquals(TICK_CNT-1,sm.get(instrument).getCount());
		assertEquals(10.0,sm.get(instrument).getMin());
		assertEquals(40.0,sm.get(instrument).getMax());
		assertEquals(25.0,sm.get(instrument).getAvg());

		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		sm = sw.getStats();
		assertEquals(TICK_CNT-2,sm.get(instrument).getCount());
		assertEquals(10.0,sm.get(instrument).getMin());
		assertEquals(30.0,sm.get(instrument).getMax());
		assertEquals(20.0,sm.get(instrument).getAvg());

		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		sm = sw.getStats();
		assertEquals(TICK_CNT-3,sm.get(instrument).getCount());
		assertEquals(10.0,sm.get(instrument).getMin());
		assertEquals(20.0,sm.get(instrument).getMax());
		assertEquals(15.0,sm.get(instrument).getAvg());

		sw.stop();
	}


}
