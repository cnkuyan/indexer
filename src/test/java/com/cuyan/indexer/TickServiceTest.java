package com.cuyan.indexer;

import com.cuyan.indexer.model.Tick;
import com.cuyan.indexer.model.TickStats;
import com.cuyan.indexer.service.TickService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.ZonedDateTime;
import java.util.Map;

import static com.cuyan.indexer.IndexerApplication.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class TickServiceTest {
	private static final long SLIDINGWINDOW_DURATION_SECS_TEST = 4;


	//@Test
	//void contextLoads() {
	//}

	private TickService tickService = null;

	@BeforeEach
	public void setup() {
		tickService = new TickService(16,SLIDINGWINDOW_DURATION_SECS_TEST * MSECS_PER_SECOND, MSECS_PER_SECOND / 10);
	}

	@AfterEach
	public void shutdown() {
		tickService.stop();
	}

	@Test
	public void testLateTickerEntry() throws InterruptedException {


		ZonedDateTime start_time = ZonedDateTime.now();
		long epoch_msecs_till_now = start_time.toEpochSecond() * MSECS_PER_SECOND;

		String instrument = "XYZ";
		Tick aLatePriceEqualTick = new Tick(instrument,10.0,
						epoch_msecs_till_now - (long)( (SLIDINGWINDOW_DURATION_SECS_TEST + 1) * MSECS_PER_SECOND));

		ResponseEntity response = tickService.add(aLatePriceEqualTick);
		assertEquals(new ResponseEntity<>(null, HttpStatus.NO_CONTENT), response);


	}

	@Test
	public void testValidTickSeriesSingleInstrument() throws InterruptedException {


		ZonedDateTime start_time = ZonedDateTime.now();
		long epoch_msecs_till_now = start_time.toEpochSecond() * MSECS_PER_SECOND;

		String instrument = "XYZ";
		Tick aPriceEqualTick_2_half_secs_ago = new Tick(instrument,10.0, epoch_msecs_till_now - (long)(2.5 * MSECS_PER_SECOND));
		Tick aPriceEqualTick_1Sec_ago = new Tick(instrument,30.0, epoch_msecs_till_now - (1 * MSECS_PER_SECOND));
		Tick aPriceEqualTick_now = new Tick(instrument,50.0, epoch_msecs_till_now);


		ResponseEntity response = tickService.add(aPriceEqualTick_2_half_secs_ago);
		assertEquals(new ResponseEntity<>(null, HttpStatus.OK), response);

		response = tickService.add(aPriceEqualTick_1Sec_ago);
		assertEquals(new ResponseEntity<>(null, HttpStatus.OK), response);

		response = tickService.add(aPriceEqualTick_now);
		assertEquals(new ResponseEntity<>(null, HttpStatus.OK), response);

		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		Map<String, TickStats> sm = tickService.stats();
		assertEquals(3,sm.get(instrument).getCount());
		assertEquals(10.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(30.0,sm.get(instrument).getAvg());

		Thread.sleep((long) 2 * MSECS_PER_SECOND);

		sm = tickService.stats();
		assertEquals(2,sm.get(instrument).getCount());
		assertEquals(30.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(40.0,sm.get(instrument).getAvg());


		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		sm = tickService.stats();
		assertEquals(1,sm.get(instrument).getCount());
		assertEquals(50.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(50.0,sm.get(instrument).getAvg());


		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		sm = tickService.stats();

		assertNotNull(sm);
		assertNull(sm.get(instrument));

	}



	@Test
	public void testValidAnachronTickSeriesSingleInstrument() throws InterruptedException {


		ZonedDateTime start_time = ZonedDateTime.now();
		long epoch_msecs_till_now = start_time.toEpochSecond() * MSECS_PER_SECOND;

		String instrument = "XYZ";
		Tick aPriceEqualTick_2_half_secs_ago = new Tick(instrument,10.0, epoch_msecs_till_now - (long)(2.5 * MSECS_PER_SECOND));
		Tick aPriceEqualTick_1Sec_ago = new Tick(instrument,30.0, epoch_msecs_till_now - (1 * MSECS_PER_SECOND));
		Tick aPriceEqualTick_now = new Tick(instrument,50.0, epoch_msecs_till_now);


		// The order of adding to the service is different from the previous test
		ResponseEntity response = tickService.add(aPriceEqualTick_now);
		assertEquals(new ResponseEntity<>(null, HttpStatus.OK), response);

		response = tickService.add(aPriceEqualTick_1Sec_ago);
		assertEquals(new ResponseEntity<>(null, HttpStatus.OK), response);


		response = tickService.add(aPriceEqualTick_2_half_secs_ago);
		assertEquals(new ResponseEntity<>(null, HttpStatus.OK), response);

		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		Map<String, TickStats> sm = tickService.stats();
		assertEquals(3,sm.get(instrument).getCount());
		assertEquals(10.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(30.0,sm.get(instrument).getAvg());

		Thread.sleep((long) 2 * MSECS_PER_SECOND);

		sm = tickService.stats();
		assertEquals(2,sm.get(instrument).getCount());
		assertEquals(30.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(40.0,sm.get(instrument).getAvg());


		Thread.sleep((long) 1.2 * MSECS_PER_SECOND);

		sm = tickService.stats();
		assertEquals(1,sm.get(instrument).getCount());
		assertEquals(50.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(50.0,sm.get(instrument).getAvg());


		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		sm = tickService.stats();

		assertNotNull(sm);
		assertNull(sm.get(instrument));

	}

}
