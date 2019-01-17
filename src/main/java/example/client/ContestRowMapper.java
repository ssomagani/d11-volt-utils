package example.client;

import java.util.ArrayList;
import java.util.function.Function;

import org.voltdb.VoltTable;

public class ContestRowMapper implements Function<VoltTable, ArrayList<Object>> {

	@Override
	public ArrayList<Object> apply(VoltTable table) {
		ArrayList<Object> resultObjects = new ArrayList<Object>();
		while(table.advanceRow()) {
			resultObjects.add(table.getLong(0));
		}
		return resultObjects;
	}
}
