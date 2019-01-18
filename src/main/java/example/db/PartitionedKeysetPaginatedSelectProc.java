package example.db;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class PartitionedKeysetPaginatedSelectProc extends VoltProcedure {

	private final SQLStmt SELECT_STMT = new SQLStmt("select * from contest_master where "
			+ "round_id = ? and contest_id > ? order by contest_id, round_id limit ?");

	public VoltTable[] run(long fakePartitionValue, int round_id, VoltTable paginationValues) {
		if(paginationValues.advanceRow()) {
			int batchSize = (int) paginationValues.getLong("BATCH_SIZE");
			int lastContestId = (int) paginationValues.getLong("KEY_1");
			voltQueueSQL(SELECT_STMT, round_id, lastContestId, batchSize);
		}
		return voltExecuteSQL(true);
	}
}
