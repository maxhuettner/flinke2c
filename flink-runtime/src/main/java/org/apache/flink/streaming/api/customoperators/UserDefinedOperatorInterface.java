package org.apache.flink.streaming.api.customoperators;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.streaming.api.customoperators.repository.EventRepository;
import org.apache.flink.streaming.api.customoperators.repository.InfinispanRepository;
import org.apache.flink.streaming.api.customoperators.repository.RedisPubSubRepository;
import org.apache.flink.streaming.api.customoperators.repository.RedisRepository;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Client-side interface between a Flink operator and the CEPless node manager: requests a
 * user-defined operator to be deployed, and routes events to/from it via the configured {@link
 * EventRepository} (Redis or Infinispan).
 */
public class UserDefinedOperatorInterface implements Serializable, OperatorEventReceiver {

	static {
		// Ensure Lettuce JFR remains disabled before any client initialization occurs
		System.setProperty("io.lettuce.core.jfr", "false");
	}

	// Event queue
	transient private EventRepository repository;

	private HashMap<CustomOperatorAddress, OperatorEventReceiver> listeners = new HashMap<CustomOperatorAddress, OperatorEventReceiver>();
	private HashMap<String, CustomOperatorAddress> operators = new HashMap<String, CustomOperatorAddress>();

	Logger LOG = LoggerFactory.getLogger(UserDefinedOperatorInterface.class);

	/**
	 * Requests an operator at the local node manager to be deployed.
	 * @param operatorName Name of the operator
	 * @param callback Object to be notified as soon as deployment was successfully invoked
	 */
	public void requestOperator(String operatorName, CustomOperatorDeployed callback) throws IOException {
		String localhostname = java.net.InetAddress.getLocalHost().getHostName();
		String opRequestIdentifier = localhostname + "-" + UUID.randomUUID().toString() + "-" + operatorName;

		HashMap<String, String> data = new HashMap<>();
		data.put("name", operatorName);
		data.put("requestIdentifier", opRequestIdentifier);

		Gson gson = new Gson();
		String json = gson.toJson(data);

		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		String addr = System.getenv("NODE_MANAGER_HOST");

		HttpPost request = new HttpPost("http://" + addr + "/requestOperator");
		StringEntity params = new StringEntity(json);
		request.addHeader("content-type", "application/json");
		request.setEntity(params);
		HttpResponse response = httpClient.execute(request);
		LOG.info("Node manager response: {}", response);

		String jsonString = EntityUtils.toString(response.getEntity());
		Type type = new TypeToken<Map<String, String>>(){}.getType();
		Map<String, String> result = gson.fromJson(jsonString, type);

		String addrIn = result.get("addrIn");
		String addrOut = result.get("addrOut");
		LOG.info("Using addrIn " + addrIn + " and addrOut " + addrOut + " for operator request " + opRequestIdentifier);

		CustomOperatorAddress address = new CustomOperatorAddress(addrIn, addrOut);
		operators.put(opRequestIdentifier, address);
		callback.notifyOperatorDeployed(opRequestIdentifier, address);
		this.repository = getRepository();
		this.repository.listen(addrOut);
	}

	/**
	 * Sends an event to a UD operator.
	 */
	public void sendEvent(String value, CustomOperatorAddress address) {
		if (this.repository == null) {
			this.repository = getRepository();
		}
		this.repository.send(address.addrIn, value);
	}

	/**
	 * Adds an event listener for new UD operator events.
	 * @return whether the listener was added
	 */
	public boolean addListener(CustomOperatorAddress operatorAddress, OperatorEventReceiver receiver) {
		LOG.info("Adding listener for address " + operatorAddress.addrOut);
		if (listeners.get(operatorAddress) != null && listeners.get(operatorAddress).equals(receiver)) {
			return false;
		}
		listeners.put(operatorAddress, receiver);
		return true;
	}

	/**
	 * Returns an instance of the event queue based on the DB_TYPE environment variable
	 * (infinispan, redis-pubsub, or redis list-based, which is the default).
	 */
	private EventRepository getRepository() {
		String dbType = System.getenv("DB_TYPE");
		if (dbType != null && dbType.equals("infinispan")) {
			String host = System.getenv("INFINISPAN_HOST");
			return new InfinispanRepository(this, host, 11222);
		} else if (dbType != null && dbType.equals("redis-pubsub")) {
			String host = System.getenv("REDIS_HOST");
			return new RedisPubSubRepository(this, host, 6379);
		} else {
			String host = System.getenv("REDIS_HOST");
			return new RedisRepository(this, host, 6379);
		}
	}

	@Override
	public void receivedEvent(String event) {
		listeners.forEach((k, v) -> {
			listeners.get(k).receivedEvent(event);
		});
	}
}
