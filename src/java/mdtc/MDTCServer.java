package mdtc;

import java.net.InetSocketAddress;

import org.apache.cassandra.service.CassandraDaemon.Server;

public class MDTCServer implements Server {
	private final InetSocketAddress socket;
	public MDTCServer(InetSocketAddress socket){
		this.socket = socket;
	}
	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
		return false;
	}

}
