package mdtc;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.CassandraDaemon;

/**
 * The <code>MDTCDaemon</code> is an extension for a Cassandra daemon service,
 * which manages Cassandra lifecycle as well as provides a HTTP server for the
 * MDTC.
 * 
 * @author qinjin.wang
 *
 */
public class MDTCDaemon extends CassandraDaemon {
	public static final int MDTC_PORT = 8513;
	public Server mdtcServer;

	private static final MDTCDaemon instance = new MDTCDaemon();

	public MDTCDaemon() {
		super(false);
	}

	protected void setup() {
		super.setup();

		// MDTC transport
		InetAddress addr = DatabaseDescriptor.getRpcAddress();
		mdtcServer = new MDTCServer(new InetSocketAddress(addr, MDTC_PORT));

	}

	public void start() {
		super.start();
		mdtcServer.start();
	}

	public void stop() {
		super.stop();
		mdtcServer.stop();
	}

	public static void main(String[] args) {
		instance.activate();
	}
}
