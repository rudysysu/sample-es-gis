package com.rudysysu.es.transportclient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BulkProcessorIndex {
	private static final Logger LOG = LoggerFactory.getLogger(BulkProcessorIndex.class);

	private static final int TOTAL_RECORD_NUM = 10000 * 2000;
	private static final int THREAD_NUM = 5;
	private static final int BATCH_SIZE = TOTAL_RECORD_NUM / THREAD_NUM;

	private static long startTime = 0;
	static {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		try {
			Date date = sdf.parse("2019-03-27T00:00:00.000Z");
			startTime = date.getTime();
		} catch (ParseException e) {
			LOG.error(e.toString(), e);
		}
	}
	private static final int MAX_OFFSET = 1000 * 60 * 60 * 24;

	private static final AtomicInteger progress = new AtomicInteger(0);

	private static final CountDownLatch latch = new CountDownLatch(THREAD_NUM);

	private static volatile boolean stopped = false;

	public static void main(String[] args) throws InterruptedException {
		startStatisticThread();

		for (int i = 0; i < THREAD_NUM; i++) {
			final int start = i * BATCH_SIZE + 1;
			final int end = (i + 1) * BATCH_SIZE;
			new Thread("WORKER-" + i) {
				public void run() {
					try {
						indexDocumentInBulk(start, end);
					} catch (InterruptedException e) {
						LOG.error(e.toString(), e);
					}
				}
			}.start();
		}

		latch.await();
		stopped = true;
	}

	private static void indexDocumentInBulk(int start, int end) throws InterruptedException {
		LOG.info("start: {}, end: {}", start, end);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		Random random = new Random();

		try (TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)) {
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.37.131"), 9300));

			BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
				@Override
				public void beforeBulk(long executionId, BulkRequest request) { // ...
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) { // ...
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, Throwable failure) { // ...
					LOG.error(failure.toString(), failure);
				}
			})// @formatter:off
					.setBulkActions(10000).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
					.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
					.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
			// @formatter:on

			for (int i = start; i <= end; i++) {
				String timestamp = sdf.format(new Date(startTime + random.nextInt(MAX_OFFSET)));

				Map<String, Integer> position = new HashMap<>();
				position.put("lat", random.nextInt(90));
				position.put("lon", random.nextInt(180));

				// @formatter:off
				bulkProcessor.add(new IndexRequest("gis-2019-02-27", "gisdata", "" + i)
						.source(jsonBuilder().startObject()
							.field("driverLicenseNum", "driverLicenseNum_" + i)
							.field("driverName", "driverName_" + i)
							.field("enterpriseId", "enterpriseId_" + i)
							.field("enterpriseName", "enterpriseName_" + i)
							.field("enterpriseType", "enterpriseType_" + i)
							.field("fleetId", "fleetId_" + i)
							.field("oemId", "oemId_" + i)
							.field("position", position)
							.field("timestamp", timestamp)
							.field("vehicleType", "vehicleType_" + i)
							.field("vin", "vin_" + i)
							.field("vrn", "vrn_" + i)
						.endObject()));
				// @formatter:on

				progress.incrementAndGet();
			}
		} catch (IOException e) {
			LOG.error(e.toString(), e);
		} finally {
			latch.countDown();
		}
	}

	private static void startStatisticThread() {
		new Thread() {
			public void run() {
				try {
					long start = System.currentTimeMillis();
					while (true) {
						int current = progress.get();
						int ratio = current * 100 / TOTAL_RECORD_NUM;
						int duration = (int) ((System.currentTimeMillis() - start) / 1000);
						int rate = 0;
						if (duration > 0) {
							rate = current / duration;
						} else if (duration < 0) {
							LOG.error("duration < 0");
						}
						LOG.info("progress: {} - {}%, rate/s: {}", current, ratio, rate);
						if (!stopped) {
							Thread.sleep(5000);
						} else {
							break;
						}
					}
				} catch (InterruptedException e) {
					LOG.error(e.toString(), e);
				}
			}
		}.start();
	}
}
