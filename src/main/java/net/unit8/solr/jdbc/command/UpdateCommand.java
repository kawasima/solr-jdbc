package net.unit8.solr.jdbc.command;

import net.unit8.solr.jdbc.expression.Item;
import net.unit8.solr.jdbc.expression.Literal;
import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.expression.ValueExpression;
import net.unit8.solr.jdbc.impl.AbstractResultSet;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.parser.ConditionParser;
import net.unit8.solr.jdbc.parser.ExpressionParser;
import net.unit8.solr.jdbc.util.SolrDocumentUtil;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.util.*;

/**
 * UPDATE command.
 *
 * @author kawasima
 */
public class UpdateCommand extends Command {
	private transient final Update updStmt;
	private ConditionParser conditionParser;
	private List<Item> setItemList;

	/** UPDATE文でSET句で指定されているカラム */
	private final Map<String, Integer> solrColumnNames;

	public UpdateCommand(Update stmt) {
		this.updStmt = stmt;
		this.parameters = new ArrayList<Parameter>();
		solrColumnNames = new HashMap<String, Integer>();
	}

	@Override
	public boolean isQuery() {
		return false;
	}

	@Override
	public AbstractResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	/**
	 * execute update query.
	 */
	@Override
	public int executeUpdate() {
		SolrQuery query = new SolrQuery(conditionParser.getQuery(parameters));
		QueryResponse response = null;
		try {
			response = conn.getSolrServer().query(query);
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e, e.getMessage());
		}
		// 対象件数が0件の場合は更新を行わずに0を返す
		if (response.getResults().getNumFound() == 0) {
			return 0;
		}

		SolrDocumentList docList = response.getResults();
		List<SolrInputDocument> inDocs = new ArrayList<SolrInputDocument>();
		Iterator<SolrDocument> iter = docList.iterator();
		while(iter.hasNext()) {
			SolrDocument doc = iter.next();
			SolrInputDocument inDoc = new SolrInputDocument();

			for(String fieldName : doc.getFieldNames()) {
				// UPDATEのSET句に含まれるカラムは、その値をセットする
				if (solrColumnNames.containsKey(fieldName)) {
                    int columnOrder = solrColumnNames.get(fieldName);
					SolrDocumentUtil.setValue(inDoc, fieldName, setItemList.get(columnOrder).getValue());
				} else if (StringUtils.equals(fieldName, "_version_")) {
                    inDoc.removeField("_version_");
                }
				// そうでない場合は、SELECTしてきた値をそのままセットする
				else {
					inDoc.setField(fieldName, doc.getFieldValue(fieldName));
				}
			}
			inDocs.add(inDoc);
		}

		try {
			conn.getSolrServer().add(inDocs);
		} catch (Exception e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e, e.getLocalizedMessage());
		}

		return inDocs.size();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void parse() {
		DatabaseMetaDataImpl metaData= this.conn.getMetaDataImpl();
		String tableName = updStmt.getTable().getName();
		if(!metaData.hasTable(tableName))
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, tableName);

		// parse SET clause.
		setItemList = new ArrayList<Item>();
		ExpressionParser expressionParser = new ExpressionParser();
		for(Expression expr : (List<Expression>)updStmt.getExpressions()) {
			expr.accept(expressionParser);
			if (expressionParser.isParameter()) {
				Parameter p = new Parameter(parameters.size());
				parameters.add(p);
				setItemList.add(p);
			} else {
				if (expressionParser.getExpression() instanceof ValueExpression) {
					setItemList.add(new Literal(((ValueExpression)expressionParser.getExpression()).getValue()));
				} else {
					DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "itemList must be literal.");
				}
			}
		}

		// parse WHERE clause.
		conditionParser = new ConditionParser(metaData, parameters);
		conditionParser.setTableName(tableName);
		updStmt.getWhere().accept(conditionParser);
		parameters = conditionParser.getParameters();

        int columnOrder = 0;
		for(Column column : (List<Column>)updStmt.getColumns()) {
			net.unit8.solr.jdbc.expression.Expression solrColumn = metaData.getSolrColumn(tableName, column.getColumnName());
			solrColumnNames.put(solrColumn.getSolrColumnName(), columnOrder++);
		}

	}
}
