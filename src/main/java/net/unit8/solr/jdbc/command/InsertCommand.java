package net.unit8.solr.jdbc.command;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.Item;
import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.impl.AbstractResultSet;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.parser.ItemListParser;
import net.unit8.solr.jdbc.util.SolrDocumentUtil;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class InsertCommand extends Command {

	private transient final Insert insStmt;
	private List<Item> itemList;

	public InsertCommand(Insert stmt) {
		this.insStmt = stmt;
		this.parameters = new ArrayList<Parameter>();
	}

	@Override
	public boolean isQuery() {
		return false;
	}

	@Override
	public void parse() {
		ItemListParser itemListParser = new ItemListParser();
		insStmt.getItemsList().accept(itemListParser);
		parameters = itemListParser.getParameters();
		itemList = itemListParser.getItemList();
	}

	@Override
	public AbstractResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int executeUpdate() {
		DatabaseMetaDataImpl metaData = this.conn.getMetaDataImpl();
		String tableName = insStmt.getTable().getName();
		List<Expression> columns = metaData.getSolrColumns(tableName);
		if (columns == null)
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, tableName);

		if (insStmt.getColumns() != null) {
			columns = new ArrayList<Expression>();
			for (Column column : (List<Column>) insStmt.getColumns()) {
				Expression solrColumn = metaData.getSolrColumn(tableName,
						column.getColumnName());
				if (solrColumn == null)
					throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, column
							.getColumnName());
				columns.add(solrColumn);
			}
		}

		if (columns.size() != parameters.size()) {
			throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
		}

		SolrInputDocument doc = new SolrInputDocument();
		for (int i = 0; i < columns.size(); i++) {
			Item item = itemList.get(i);
			SolrDocumentUtil.setValue(doc, columns.get(i).getSolrColumnName(),
					item.getValue());
		}
		doc
				.setField("id", "@" + metaData.getOriginalTableName(tableName) + "."
						+ UUID.randomUUID().toString());
		try {
			conn.getSolrServer().add(doc);
		} catch (Exception e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e,
					"Solr Server Error");
		}

		return doc.size();
	}
}
