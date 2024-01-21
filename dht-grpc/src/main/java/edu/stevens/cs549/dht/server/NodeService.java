package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.activity.IDhtNode;
import edu.stevens.cs549.dht.activity.IDhtService;
import edu.stevens.cs549.dht.events.EventProducer;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceImplBase;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService extends DhtServiceImplBase {
	
	private static final String TAG = NodeService.class.getCanonicalName();
	
	private static Logger logger = Logger.getLogger(TAG);

	/**
	 * Each service request is processed by a distinct service object.
	 *
	 * Shared state is in the state object; we use the singleton pattern to make sure it is shared.
	 */
	private IDhtService getDht() {
		return Dht.getDht();
	}
	
	// TODO: add the missing operations
	public void getPred(Empty empty, StreamObserver<OptNodeInfo> responseObserver) {
		Log.weblog(TAG, "getPred()");
		responseObserver.onNext(getDht().getPred());
		responseObserver.onCompleted();
	}
	public void getSucc(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getSucc()");
		responseObserver.onNext(getDht().getSucc());
		responseObserver.onCompleted();
	}
	public void closestPrecedingFinger(Id i, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "closestPrecedingFinger()");
		responseObserver.onNext(getDht().closestPrecedingFinger(i.getId()));
		responseObserver.onCompleted();
	}
	public void notify(NodeBindings b, StreamObserver<OptNodeBindings> responseObserver) {
		Log.weblog(TAG, "notify()");
		responseObserver.onNext(getDht().notify(b));
		responseObserver.onCompleted();
	}
	public void getBindings(Key k, StreamObserver<Bindings> responseObserver) {
		try {
			Log.weblog(TAG, "getBindings()");
			String[] s = getDht().get(k.getKey());
			Bindings b = null;
			if(s!=null)
				b = Bindings.newBuilder().setKey(k.getKey()).addAllValue(Arrays.asList(s)).build();
			responseObserver.onNext(b);
			responseObserver.onCompleted();
		}catch (Invalid e){
			Log.weblog(TAG, e.getMessage());
		}
	}
	public void addBinding(Binding b, StreamObserver<Empty> responseObserver) {
		try {
			Log.weblog(TAG, "addBinding()");
			getDht().add(b.getKey(), b.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}catch (Invalid e){
			Log.weblog(TAG, e.getMessage());
		}
	}
	public void deleteBinding(Binding b, StreamObserver<Empty> responseObserver) {
		try {
			Log.weblog(TAG, "deleteBinding()");
			getDht().delete(b.getKey(), b.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}catch (Invalid e){
			Log.weblog(TAG, e.getMessage());
		}
	}
	public void findSuccessor(Id i, StreamObserver<NodeInfo> responseObserver) {
		try {
			Log.weblog(TAG, "findSuccessor()");
			responseObserver.onNext(getDht().findSuccessor(i.getId()));
			responseObserver.onCompleted();
		}catch (Failed e){
			Log.weblog(TAG, e.getMessage());
		}
	}

	public void listenOn(Subscription i, StreamObserver<Event> responseObserver) {
		Log.weblog(TAG, "listenOn()");
		getDht().listenOn(i.getId(), i.getKey(), EventProducer.create(responseObserver));
	}

	public void listenOff(Subscription i, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "listenOff()");
		getDht().listenOff(i.getId(), i.getKey());
		responseObserver.onNext(Empty.getDefaultInstance());
		responseObserver.onCompleted();
	}
	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

	@Override
	public void getNodeInfo(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getNodeInfo()");
		responseObserver.onNext(getDht().getNodeInfo());
		responseObserver.onCompleted();
	}


}