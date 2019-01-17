import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.client.ClientResponseWithPartitionKey;

import mock.MockClient;
import client.utils.RetryScatterCall;
import client.utils.RetryScatterCall.RetryScatterCallback;

public class RetryScatterCallTest {

	@Before
	public void setUp() throws Exception {
		
	}

	final int MAX_RETRY = 3;
	
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
		Object[] args = {null};
		call.invoke(callback, "", args);
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
		Object[] args = {null};
		call.invoke(callback, "", args);
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
		Object[] args = {null};
		call.invoke(callback, "", args);
	}
}
