package net.unit8.solr.jdbc.command;

import net.sf.jsqlparser.statement.drop.Drop;
import net.unit8.solr.jdbc.impl.AbstractResultSet;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import org.apache.solr.client.solrj.response.UpdateResponse;


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
			UpdateResponse response = conn.getSolrServer().deleteByQuery(
					"meta.name:" + tableName + " OR id:@" + tableName + ".*");
            if (response.getStatus() != 0)
                throw DbException.get(ErrorCode.GENERAL_ERROR, "Solr Server status is " + response.getStatus());
			conn.setUpdatedInTx(true);
            conn.setSoftCommit(false);
			conn.commit();
			conn.refreshMetaData();
		} catch(Exception e) {
			DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error");
		}
		return 0;
	}

}
