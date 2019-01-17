package client.utils;

import java.io.IOException;

import org.voltdb.client.AllPartitionProcedureCallback;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.ProcCallException;

public class RetryScatterCall {

	private final Client client;
	private final int MAX_TRIES;
	private int numTries = 1;

	public RetryScatterCall(Client client, int maxTries) {
		this.client = client;
		MAX_TRIES = maxTries;
	}

	public void invoke(RetryScatterCallback callback, String proc, Object... args) {
		InternalRetryScatterCallback internalCallback = new InternalRetryScatterCallback(callback, proc, args);
		try {
			client.callAllPartitionProcedure(internalCallback, proc, args);
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
	public static abstract class RetryScatterCallback implements AllPartitionProcedureCallback {
		private Boolean success;
		public Boolean success() {return success;}
	}
	
	public class InternalRetryScatterCallback implements AllPartitionProcedureCallback {

		private String proc;
		private Object[] args;
		private RetryScatterCallback callback;

		public InternalRetryScatterCallback(RetryScatterCallback callback, String proc, Object... args) {
			this.proc = proc;
			this.args = args;
			this.callback = callback;
		}
		
		@Override
		public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {
			for (ClientResponseWithPartitionKey response : responses) {
				if(response.response.getStatus() != ClientResponse.SUCCESS) {
					if(numTries < MAX_TRIES) {
						numTries++;
						invoke(callback, proc, args);
						return;
					} else {
						callback.success = Boolean.FALSE;
						callback.clientCallback(responses);
						return;
					}
				}
			}
			callback.success = Boolean.TRUE;
			callback.clientCallback(responses);
		}
	}
}
