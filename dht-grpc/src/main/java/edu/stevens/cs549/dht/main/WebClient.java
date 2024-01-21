package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.events.EventConsumer;
import edu.stevens.cs549.dht.events.IEventListener;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceBlockingStub;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceStub;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import java.util.logging.Level;
import java.util.logging.Logger;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private IChannels channels;

	private WebClient(IChannels channels) {
		this.channels = channels;
	}

	public static WebClient getInstance(IState state) {
		return new WebClient(state.getChannels());
	}

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	/*
	 * Get a blocking stub (channels and stubs are cached for reuse).
	 */
	private DhtServiceBlockingStub getStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceBlockingStub getStub(NodeInfo target) throws DhtBase.Failed {
		return getStub(target.getHost(), target.getPort());
	}

	private DhtServiceStub getListenerStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newStub(channel);
	}

	private DhtServiceStub getListenerStub(NodeInfo target) throws DhtBase.Failed {
		return getListenerStub(target.getHost(), target.getPort());
	}


	/*
	 * TODO: Fill in missing operations.
	 */
	public NodeInfo getNodeInfo(NodeInfo node) throws DhtBase.Failed{
		return getStub(node).getNodeInfo(Empty.getDefaultInstance());
	}

	public NodeInfo getSucc(NodeInfo node) throws DhtBase.Failed{
		return getStub(node).getSucc(Empty.getDefaultInstance());
	}

	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) throws DhtBase.Failed{
		Id i = Id.newBuilder().setId(id).build();
		return getStub(node).closestPrecedingFinger(i);
	}

	public String[] get(NodeInfo node, String k) throws DhtBase.Failed{

		Key i = Key.newBuilder().setKey(k).build();
		Bindings b = getStub(node).getBindings(i);
		return b.getValueList().toArray(new String[0]);
	}

	public void add(NodeInfo node, String k, String v) throws DhtBase.Failed{
		Binding i = Binding.newBuilder().setKey(k).setValue(v).build();
		getStub(node).addBinding(i);
	}

	public void delete(NodeInfo node, String k, String v) throws DhtBase.Failed{
		Binding i = Binding.newBuilder().setKey(k).setValue(v).build();
		getStub(node).deleteBinding(i);
	}

	public NodeInfo findSuccessor(NodeInfo node, int id) throws DhtBase.Failed{
		Id i = Id.newBuilder().setId(id).build();
		NodeInfo n = getStub(node).findSuccessor(i);
		if(n.getPort()==0)
			return null;
		return n;
	}
	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getPred("+node.getId()+")");
		return getStub(node).getPred(Empty.getDefaultInstance());
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		Log.weblog(TAG, "notify("+node.getId()+")");
		// TODO
		return getStub(node).notify(predDb);
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
	}

	/*
	 * Listening for new bindings.
	 */
	public void listenOn(NodeInfo node, Subscription subscription, IEventListener listener) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOn("+node.getId()+")");
		// TODO listen for updates for the key specified in the subscription
		getListenerStub(node).listenOn(subscription, EventConsumer.create(subscription.getKey(), listener));
	}

	public void listenOff(NodeInfo node, Subscription subscription) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOff("+node.getId()+")");
		// TODO stop listening for updates on bindings to the key in the subscription
		getStub(node).listenOff(subscription);
	}
	
}
