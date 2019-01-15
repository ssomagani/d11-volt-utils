import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.client.ClientResponseWithPartitionKey;

import mock.MockClient;
import utils.RetryScatterCall;
import utils.RetryScatterCall.RetryScatterCallback;

public class RetryScatterCallTest {

	final int MAX_RETRY = 3;
	
	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void testInvokeEqual() {
		MockClient client = new MockClient();
		client.setPassAt(3);
		RetryScatterCall call = new RetryScatterCall(client, MAX_RETRY);
		RetryScatterCallback callback = new RetryScatterCallback() {

			@Override
			public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {
				Boolean[] expecteds = {Boolean.TRUE};
				Boolean[] actuals = {this.success()};
				assertArrayEquals(expecteds, actuals);
			}
			
		};
		call.invoke(callback, "", null);
	}
	
	@Test
	public void testInvokePass() {
		MockClient client = new MockClient();
		client.setPassAt(2);
		RetryScatterCall call = new RetryScatterCall(client, MAX_RETRY);
		RetryScatterCallback callback = new RetryScatterCallback() {

			@Override
			public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {
				Boolean[] expecteds = {Boolean.TRUE};
				Boolean[] actuals = {this.success()};
				assertArrayEquals(expecteds, actuals);
			}
			
		};
		call.invoke(callback, "", null);
	}
	
	@Test
	public void testInvokeFail() {
		MockClient client = new MockClient();
		client.setPassAt(6);
		RetryScatterCall call = new RetryScatterCall(client, MAX_RETRY);
		RetryScatterCallback callback = new RetryScatterCallback() {

			@Override
			public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {
				Boolean[] expecteds = {Boolean.FALSE};
				Boolean[] actuals = {this.success()};
				assertArrayEquals(expecteds, actuals);
			}
			
		};
		call.invoke(callback, "", null);
	}
}
