import java.io.IOException;

import org.voltdb.client.AllPartitionProcedureCallback;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;

import utils.RetryScatterCall;
import utils.RetryScatterCall.RetryScatterCallback;

public class Application {

	public static void main(String[] args) {
		Client client = ClientFactory.createClient();
		
		try {
			client.createConnection("localhost");
			int maxRetryCount = 5;
			RetryScatterCall call = new RetryScatterCall(client, maxRetryCount);
			
			RetryScatterCallback callback = new RetryScatterCallback() {

				@Override
				public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {
					
				}
			};
			call.invoke(callback, "PROCEDURE", "asdf");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}


