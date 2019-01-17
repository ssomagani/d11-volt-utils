package example.db;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class PartitionedPaginatedSelectProc extends VoltProcedure {

	private final SQLStmt SELECT_STMT = new SQLStmt("select * from contest_master where round_id = ? order by contest_id, round_id limit ? offset ?");
	private final SQLStmt SELECT_COUNT_STMT = new SQLStmt("select count(*) from (select count(*) over() from contest_master where round_id = ? order by contest_id, round_id offset ?) t");

	private final VoltTable.ColumnInfo[] COLS = {
			new VoltTable.ColumnInfo("ROWS_NOT_SELECTED_YET", VoltType.INTEGER), 
			new VoltTable.ColumnInfo("ROWS_SELECTED", VoltType.INTEGER)
	};
	private final VoltTable STATS = new VoltTable(COLS);
	
	public VoltTable[] run(int contestId, int round_id, int batchSize, int offset) {
		VoltTable[] result = new VoltTable[2];
		STATS.clearRowData();

		// Array to hold ROWS_LEFT and ROWS_DELETED
		Integer[] statValues = {0,0};

		voltQueueSQL(SELECT_COUNT_STMT, round_id, offset+batchSize);
		voltQueueSQL(SELECT_STMT, round_id, batchSize, offset);
		
		VoltTable[] tables = voltExecuteSQL();
		VoltTable statRows = tables[0];
		VoltTable dataRows = tables[1];
		
		if(statRows.advanceRow()) {
			statValues[0] = (int) statRows.getLong(0);
		}
		
		statValues[1] = dataRows.getRowCount();
		STATS.addRow(statValues);
		
		result[0] = STATS;
		result[1] = dataRows;
		return result;
	}
}
