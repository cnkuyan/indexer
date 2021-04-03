package com.cuyan.indexer;

import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IndexerApplicationTests {


	/*
	@Test
	public void testSlidingWindowSingleInstrument() throws InterruptedException {

		SlidingWindow sw = new SlidingWindow(MAX_QUEUE_SIZE, 3 * MSECS_PER_SECOND,
												TIMER_REFRESH_MSECS );

		sw.start();

		ZonedDateTime start_time = ZonedDateTime.now();
		long epoch_msecs_till_now = start_time.toEpochSecond() * MSECS_PER_SECOND;

		String instrument = "XYZ";
		PriceEqualTick aTick_2_half_secs_ago = new PriceEqualTick(instrument,10.0, epoch_msecs_till_now - (long)(2.5 * MSECS_PER_SECOND));
		PriceEqualTick aTick_1sec_ago = new PriceEqualTick(instrument,30.0, epoch_msecs_till_now - (1 * MSECS_PER_SECOND));
		PriceEqualTick aTick_now = new PriceEqualTick(instrument,50.0, epoch_msecs_till_now);

		sw.add(aTick_2_half_secs_ago);
		sw.add(aTick_1sec_ago);
		sw.add(aTick_now);

		System.out.println("aTick_now:"+ aTick_now);



		Map<String, TickStats> sm = sw.getStats();
		assertEquals(10.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(30.0,sm.get(instrument).getAvg());

		Thread.sleep((long) 2 * MSECS_PER_SECOND);


		sm = sw.getStats();
		assertEquals(30.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(40.0,sm.get(instrument).getAvg());
		assertEquals(2,sm.get(instrument).getCount());

		Thread.sleep((long) 1.5 * MSECS_PER_SECOND);

		sm = sw.getStats();
		assertEquals(50.0,sm.get(instrument).getMin());
		assertEquals(50.0,sm.get(instrument).getMax());
		assertEquals(50.0,sm.get(instrument).getAvg());
		assertEquals(1,sm.get(instrument).getCount());


		Thread.sleep((long) 1 * MSECS_PER_SECOND);

		sm = sw.getStats();

		assertNotNull(sm);
		assertNull(sm.get(instrument));

	}//Test
	 */
}
