package net.unit8.solr.jdbc.command;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.unit8.solr.jdbc.ColumnSpec;
import net.unit8.solr.jdbc.impl.AbstractResultSet;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.DataType;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;


public class CreateTableCommand extends Command{
	private final CreateTable createTable;

	public CreateTableCommand(CreateTable statement) {
		this.createTable = statement;
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

	@SuppressWarnings("unchecked")
	@Override
	public int executeUpdate() {
		String tableName = createTable.getTable().getName();
		SolrQuery query = new SolrQuery();
		query.setQuery("meta.name:"+ tableName);
		try {
			QueryResponse response =  conn.getSolrServer().query(query);
			if (!response.getResults().isEmpty()) {
				throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS, tableName);
			}
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e);
		}
		SolrInputDocument doc = new SolrInputDocument();
		doc.setField("meta.name", createTable.getTable().getName());

		for(Object elm : createTable.getColumnDefinitions()) {
			ColumnDefinition columnDef = (ColumnDefinition)elm;
			String sqlTypeName = columnDef.getColDataType().getDataType();
			ColumnSpec spec = new ColumnSpec(columnDef.getColumnSpecStrings());
            DataType dt = DataType.getTypeByName(sqlTypeName);
            if (dt == null)
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE, sqlTypeName);
			if (spec.isArray()) {
				doc.addField("meta.columns", columnDef.getColumnName()+".M_"+dt.type.name());
			} else if (dt.sqlType == Types.ARRAY) {
                DataType originalType = DataType.getTypeByName(dt.name.replace("_ARRAY$", ""));
                if (originalType == null)
                    throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE, sqlTypeName);
                doc.addField("meta.columns", columnDef.getColumnName() + ".M_" + originalType.type.name());
            } else {
				doc.addField("meta.columns", columnDef.getColumnName()+"."+dt.type.name());
			}
		}
		doc.setField("id", UUID.randomUUID().toString());
		try {
			conn.getSolrServer().add(doc);
			conn.setUpdatedInTx(true);
			conn.commit();
			conn.refreshMetaData();
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error");
		} catch (IOException e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e);
		} catch (SQLException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e, "Commit Error");
		}
		return 0;
	}

}
