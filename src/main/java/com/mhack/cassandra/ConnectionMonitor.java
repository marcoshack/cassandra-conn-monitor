package com.mhack.cassandra;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

public class ConnectionMonitor {
	private static Logger logger = LoggerFactory.getLogger(ConnectionMonitor.class);

	public static void main(String[] args) throws InterruptedException {
		final String[] hosts = StringUtils.split(System.getProperty("hosts", "127.0.0.1"), ",");
		final int queryInterval = Integer.parseInt(System.getProperty("queryInterval", "60"));
		final String username = System.getProperty("username");
		final String password = System.getProperty("password");
		final int maxConn = Integer.parseInt(System.getProperty("maxConn", "1"));
		final int coreConn = Integer.parseInt(System.getProperty("coreConn", "1"));
		final int heartbeatInterval = Integer.parseInt(System.getProperty("heartbeatInterval", "30"));

		System.out.println("Starting Connection Monitor");
		System.out.println(String.format("hosts: %s, queryInterval: %d, authCredentials: %b, coreConn: %d, maxConn: %d, heartbeatInterval: %d", Arrays.toString(hosts), queryInterval, username != null && password != null, coreConn, maxConn, heartbeatInterval));

		final PoolingOptions poolingOptions = new PoolingOptions()
				.setConnectionsPerHost(HostDistance.LOCAL, coreConn, maxConn)
				.setConnectionsPerHost(HostDistance.REMOTE, coreConn, maxConn)
				.setHeartbeatIntervalSeconds(heartbeatInterval);

		final Cluster.Builder builder = Cluster.builder()
				.addContactPoints(hosts)
				.withPoolingOptions(poolingOptions)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 10000))
				.withSocketOptions(new SocketOptions().setReadTimeoutMillis(30000))
				.withProtocolVersion(ProtocolVersion.V3)
				.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);

		if (username != null && password != null) {
			builder.withCredentials(username, password);
		}

		final Session session = createSession(builder, logger);
		try {
			final ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
			scheduled.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						session.execute("select now() from system.local;");
						logger.info("Query OK");

					} catch (Exception queryErr) {
						logger.error("Failed to execute query (retring in {}ms): {}", queryInterval, queryErr.getMessage(), queryErr);
					}
				}
			}, 0, queryInterval, TimeUnit.SECONDS);

		} catch (Exception e) {
			logger.error("Unrecoverable error: {}", e.getMessage(), e);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.info("Shutting down");
				if (session != null && session.getCluster() != null && !session.getCluster().isClosed()) {
					session.getCluster().close();
				}
				logger.info("Done.");
			}
		});
	}
	
	private static final Session createSession(Cluster.Builder clusterBuilder, Logger logger) throws InterruptedException {
		Session session = null;
		do {
			try {
				session = clusterBuilder.build().connect();
			} catch (Exception connErr) {
				logger.error("Unable to connect to cluster (retrying in {}s): {}", 3, connErr.getMessage());
				Thread.sleep(3000);
			}
		} while (session == null);
		return session;
	}
}
