package net.unit8.solr.jdbc.command;

import net.sf.jsqlparser.statement.drop.Drop;

import net.unit8.solr.jdbc.impl.AbstractResultSet;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;


public class DropCommand extends Command {
	private Drop statement;

	protected DropCommand(Drop statement) {
		this.statement = statement;
	}

	@Override
	public boolean isQuery() {
		return false;
	}

	@Override
	public void parse() {

	}

	@Override
	public AbstractResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@Override
	public int executeUpdate() {
		DatabaseMetaDataImpl metaData = conn.getMetaDataImpl();
		String tableName = statement.getName();
		if(!metaData.hasTable(tableName))
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, tableName);
		tableName = metaData.getOriginalTableName(tableName);
		try {
			conn.getSolrServer().deleteByQuery(
					"meta.name:" + tableName + " OR id:@" + tableName + ".*");
			conn.setUpdatedInTx(true);
			conn.commit();
			conn.refreshMetaData();
		} catch(Exception e) {
			DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error");
		}
		return 0;
	}

}
