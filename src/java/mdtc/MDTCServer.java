package mdtc;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.service.CassandraDaemon.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

public class MDTCServer implements Server {
	private static final Logger logger = LoggerFactory
			.getLogger(MDTCServer.class);

	private final AtomicBoolean isRunning = new AtomicBoolean(false);
	private final InetSocketAddress socket;
	private HttpServer server;

	public MDTCServer(InetSocketAddress socket) {
		this.socket = socket;
	}

	@Override
	public void start() {
		try {
			server = HttpServer.create(socket, 0);
			server.createContext("/", new MDTCMessageHandler());
			server.setExecutor(Executors.newCachedThreadPool());
			server.start();
			isRunning.set(true);
			logger.info("MDTCServer is started on {}...", socket);
		} catch (Exception ex) {
			logger.error("Exception on start MDTCServer {}", ex.getMessage());
			ex.printStackTrace();
		}

	}

	@Override
	public void stop() {
		if (server != null) {
			server.stop(0);
			isRunning.set(false);
		}
	}

	@Override
	public boolean isRunning() {
		return isRunning.get();
	}
}
