package mdtc;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mdtc.api.json.GsonHelper;
import mdtc.api.transaction.data.NodeID;
import mdtc.api.transaction.data.TItem;
import mdtc.api.transaction.message.IMessage;
import mdtc.api.transaction.message.TranEndPointMessage;
import mdtc.utils.ApacheHTTPClientTool;

import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class MDTCMessageHandler implements HttpHandler {
	private static final Logger logger = LoggerFactory
			.getLogger(MDTCMessageHandler.class);
	private static final ExecutorService executorService = Executors
			.newSingleThreadExecutor();

	@Override
	public void handle(final HttpExchange exchange) throws IOException {

		executorService.execute(new Runnable() {

			@Override
			public void run() {
				InputStream inputStream = exchange.getRequestBody();
				try {
					handleMessage(inputStream);
					exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
				} catch (Exception ex) {
					logger.error("Exception on handling MDTC messages: {}",
							ex.getMessage());
					ex.printStackTrace();
					try {
						exchange.sendResponseHeaders(
								HttpURLConnection.HTTP_INTERNAL_ERROR, -1);
					} catch (IOException e) {
						e.printStackTrace();
					}
				} finally {
					try {
						inputStream.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					exchange.close();
				}
			}
		});
	}

	private void handleMessage(InputStream inputStream) throws Exception {
		IMessage message = GsonHelper.instance().fromJson(inputStream);
		logger.info("Handle {} message from {}", message.getMessageType(),
				message.getSenderID());
		switch (message.getMessageType()) {
		case TranEndPoint:
			onGetEndPoint((TranEndPointMessage) message);
			break;
		default:
			throw new Exception("Not supported message type "
					+ message.getMessageType());
		}
	}

	private void onGetEndPoint(TranEndPointMessage message) {
		logger.info("Handle {} message from {}", message.getMessageType(),
				message.getSenderID());
		for (TItem item : message.items) {
			List<InetAddress> endPoints = StorageService.instance
					.getNaturalEndpoints(message.keyspace,
							item.getTxnKey().columnFamily, item.getKey());
			List<NodeID> endPointIDs = Lists.newArrayList();
			for (InetAddress endPoint : endPoints) {
				endPointIDs.add(new NodeID(item.getNodeID().id, endPoint
						.getHostAddress(), item.getNodeID().port));
			}

			message.endpoints.put(item.getKey(), endPointIDs);
		}

		//Send back the endpoint info.
		NodeID temp = message.sender;
		message.sender = message.receiver;
		message.receiver = temp;
		ApacheHTTPClientTool.getInstance().sendMessage(message);
	}
}
