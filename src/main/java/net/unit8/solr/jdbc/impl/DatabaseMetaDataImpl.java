package net.unit8.solr.jdbc.impl;

import net.unit8.solr.jdbc.expression.ColumnExpression;
import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.DataType;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class DatabaseMetaDataImpl implements DatabaseMetaData {
	private final SolrConnection conn;
	private Map<String, List<Expression>> tableColumns = new HashMap<String, List<Expression>>();
    private Map<String, List<String>> tablePrimaryKeys  = new HashMap<String, List<String>>();
	private Map<String, String> originalTables = new HashMap<String, String>();

	public DatabaseMetaDataImpl(SolrConnection conn) {
		this.conn = conn;
		buildMetadata();
	}

	private void buildMetadata() {
		try {
			QueryResponse res = this.conn.getSolrServer().query(
					new SolrQuery("meta.name:*"));

			for (SolrDocument doc : res.getResults()) {
				String tableName = doc.getFieldValue("meta.name").toString();
				List<Expression> columns = new ArrayList<Expression>();
				for (Object cols : doc.getFieldValues("meta.columns")) {
					columns.add(new ColumnExpression(tableName + "."
							+ cols.toString()));
				}
                List<String> primaryKeys = new ArrayList<String>();
                Collection<Object> pkColumns = doc.getFieldValues("meta.primaryKeys");
                if (pkColumns != null) {
                    for (Object pkColumn : pkColumns) {
                        primaryKeys.add(tableName + "." + pkColumn.toString());
                    }
                }
				tableColumns.put(StringUtils.upperCase(tableName), columns);
                tablePrimaryKeys.put(StringUtils.upperCase(tableName), primaryKeys);
				originalTables.put(StringUtils.upperCase(tableName), tableName);
			}

		} catch (Exception e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e, e.getLocalizedMessage());
		}
	}

	public Expression getSolrColumn(String tableName, String columnName) {
		for (Expression solrColumn : this.tableColumns.get(StringUtils.upperCase(tableName))) {
			if (StringUtils.equals(solrColumn.getColumnName(), columnName)) {
				return solrColumn;
			}
		}
		return null;

	}

	public List<Expression> getSolrColumns(String tableName) {
		if (tableColumns == null)
			buildMetadata();
		return this.tableColumns.get(StringUtils.upperCase(tableName));
	}

	public boolean hasTable(String tableName) {
		return originalTables.containsKey(StringUtils.upperCase(tableName));
	}

	public String getOriginalTableName(String tableName) {
		return originalTables.get(StringUtils.upperCase(tableName));
	}

	/**
	 * Checks if all procedures callable.
	 *
	 * @return true
	 */
	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return true;
	}

	/**
	 * Checks if it possible to query all tables returned by getTables.
	 *
	 * @return true
	 */
	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	/**
	 * Returns whether an exception while auto commit is on closes all result
	 * sets.
	 *
	 * @return false
	 */
	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	/**
	 * Returns whether data manipulation and CREATE/DROP is supported in
	 * transactions.
	 *
	 * @return false
	 */
	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	/**
	 * Returns whether CREATE/DROP do not affect transactions.
	 *
	 * @return fasle
	 */
	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	/**
	 * Returns whether deletes are detected.
	 *
	 * @return false
	 */
	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	/**
	 * Returns whether the maximum row size includes blobs.
	 *
	 * @return false
	 */
	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getAttributes")
				.getSQLException();
	}

	@Override
	public ResultSet getBestRowIdentifier(String s, String s1, String s2,
			int i, boolean flag) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getBestRowIdentifier").getSQLException();
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getCatalogSeparator").getSQLException();
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		throw DbException
				.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getCatalogTerm")
				.getSQLException();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getCatalogs")
				.getSQLException();
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getClientInfoProperties").getSQLException();
	}

	@Override
	public ResultSet getColumnPrivileges(String s, String s1, String s2,
			String s3) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getColumnPrivileges").getSQLException();
	}

	@Override
	public ResultSet getColumns(String catalog, String schema, String table,
			String columnNamePattern) throws SQLException {
		if (tableColumns == null) {
			buildMetadata();
		}

		CollectionResultSet rs;
		String[] columns = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
				"BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
				"NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
				"SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
				"SOURCE_DATA_TYPE" };
		rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));

		for (Expression column : tableColumns.get(StringUtils.upperCase(table))) {
			Object[] columnMeta = new Object[22];
			columnMeta[1] = ""; // TABLE_SCHEM
			columnMeta[2] = column.getTableName();
			columnMeta[3] = column.getColumnName(); // COLUMN_NAME

			columnMeta[4] = DataType.getDataType(column.getType()).sqlType; // DATA_TYPE
			columnMeta[5] = column.getTypeName(); // TYPE_NAME
			columnMeta[6] = 0; // COLUMN_SIZE
			columnMeta[8] = 0; // DECIMAL_DIGITS
			columnMeta[10] = DatabaseMetaData.columnNullableUnknown; // NULLABLE
			rs.add(Arrays.asList(columnMeta));
		}

		return rs;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public ResultSet getCrossReference(String s, String s1, String s2,
			String s3, String s4, String s5) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getCrossReference");
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "solr";
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return 1;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return "0.1.4-SNAPSHOT";
	}

	/**
	 * Returns default transaction isolation.
	 *
	 * @return Connection.TRANSACTION_READ_COMMITTED
	 */
	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_READ_COMMITTED;
	}

	@Override
	public int getDriverMajorVersion() {
		return 0;
	}

	@Override
	public int getDriverMinorVersion() {
		return 1;
	}

	@Override
	public String getDriverName() throws SQLException {
		return "solr-jdbc";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return "0.1.4-SNAPSHOT";
	}

	@Override
	public ResultSet getExportedKeys(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getExportedKeys");
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getExtraNameCharacters");
	}

	@Override
	public ResultSet getFunctionColumns(String s, String s1, String s2,
			String s3) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getFunctionColumns");
	}

	@Override
	public ResultSet getFunctions(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getFunctions");
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getFunctionColumns");
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		CollectionResultSet rs = new CollectionResultSet();
		String[] columns = { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
				"PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
				"FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
				"DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY" };
		rs.setColumns(Arrays.asList(columns));
		return rs;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		CollectionResultSet rs = new CollectionResultSet();
		String[] columns = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
				"ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
				"CARDINALITY", "PAGES", "FILTER_CONDITION" };
		rs.setColumns(Arrays.asList(columns));
		return rs;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return 2;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getMaxBinaryLiteralLength");
	}

	/**
	 * Returns the maximum length for a catalog name.
	 *
	 * @return 0 for limit is unknown.
	 */
	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	/**
	 * Returns the maximum length of literals.
	 *
	 * @return 0 for limit is unknown
	 */
	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	/**
	 * Returns the maximum length of column names.
	 *
	 * @return 0 for limit is unknown
	 */
	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	/**
	 * SolrのFacetの仕様のため、1を返す.
	 *
	 */
	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0; // No limitation
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 0; // No limitation
	}

	/**
	 * Returns the maximum length of a statement.
	 *
	 * @return 0 for limit is unknown
	 */
	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	/**
	 * Returns the maximum number of open statements.
	 *
	 * @return 0 for limit is unknown
	 */
	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getNumericFunctions");
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		CollectionResultSet rs;
		String[] columns = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"COLUMN_NAME", "KEY_SEQ", "PK_NAME" };
		rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));


        short keySeq = 0;
		for (String primaryKey : tablePrimaryKeys.get(StringUtils.upperCase(table))) {
            String[] pkTokens = StringUtils.split(primaryKey, ".", 2);
			Object[] columnMeta = new Object[6];
			columnMeta[2] = pkTokens[0];
			columnMeta[3] = pkTokens[1]; //COLUMN_NAME
			columnMeta[4] = keySeq++; //KEY_SEQ
			columnMeta[5] = null; // PK_NAME
			rs.add(Arrays.asList(columnMeta));
		}
		return rs;
	}

	@Override
	public ResultSet getProcedureColumns(String s, String s1, String s2,
			String s3) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getProcedureColumns");
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getProcedureTerm");
	}

	@Override
	public ResultSet getProcedures(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getProcedureTerm");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		throw DbException
				.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSQLKeywords");
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return sqlStateXOpen;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSchemas");
	}

	@Override
	public ResultSet getSchemas(String s, String s1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSchemas");
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "%";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getStringFunctions");
	}

	@Override
	public ResultSet getSuperTables(String s, String s1, String s2)
			throws SQLException {
		throw DbException
				.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSuperTables");
	}

	@Override
	public ResultSet getSuperTypes(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSuperTypes");
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getSystemFunctions");
	}

	@Override
	public ResultSet getTablePrivileges(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getTablePrivileges");
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		String[] columns = { "TABLE_TYPE" };
		CollectionResultSet rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));
		Object[] record = { "TABLE" };
		rs.add(Arrays.asList(record));
		return rs;
	}

	@Override
	public ResultSet getTables(String catalog, String schema,
			String tableNamePattern, String[] types) throws SQLException {
		if (tableColumns == null) {
			buildMetadata();
		}
        if (tableNamePattern == null)
            tableNamePattern = "%";

		CollectionResultSet rs;
		String[] columns = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SELF_REFERENCING_COL_NAME", "REF_GENERATION" };
		rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));

		Pattern ptn = getPattern(tableNamePattern);
		for (String tableName : tableColumns.keySet()) {
			if(ptn.matcher(tableName).matches()) {
				Object[] tableMeta = { null, "", tableName, "TABLE", "", null,
					null, null, null, null };
				rs.add(Arrays.asList(tableMeta));
			}
		}

		return rs;
	}

	protected Pattern getPattern(String ptnStr) {
		ptnStr = ptnStr.replaceAll("(?<!\\\\)_", ".?").replaceAll("(?<!\\\\)%", ".*?");
		return Pattern.compile("^" + ptnStr + "$", Pattern.CASE_INSENSITIVE);
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getTypeInfo");
	}

	@Override
	public ResultSet getUDTs(String s, String s1, String s2, int[] ai)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getUDTs");
	}

	@Override
	public String getURL() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getURL");
	}

	@Override
	public String getUserName() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getUserName");
	}

	@Override
	public ResultSet getVersionColumns(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"getVersionColumns");
	}

	/**
	 * Returns whether inserts are detected.
	 */
	@Override
	public boolean insertsAreDetected(int i) throws SQLException {
		return false;
	}

	/**
	 * Returns whether the catalog is at the beginning.
	 *
	 * @return true
	 */
	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	/**
	 * Returns the same as Connection.isReadOnly().
	 *
	 * @return if read only optimization is switched on
	 */
	@Override
	public boolean isReadOnly() throws SQLException {
		return conn.isReadOnly();
	}

	/**
	 * Does the database make a copy before updating.
	 *
	 * @return false
	 */
	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	/**
	 * Returns whether NULL+1 is NULL or not.
	 *
	 * @return true
	 */
	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	/**
	 * Checks if NULL is sorted at the beginning (no matter if ASC or DESC is used).
	 *
	 * @return false
	 */
	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	/**
	 * Checks if NULL is sorted at the end (no matter if ASC or DESC is used).
	 *
	 * @return false
	 */
	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	/**
	 * Checks if NULL is sorted high (bigger than anything that is not null).
	 */
	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	/**
	 * Checks if NULL is sorted low (bigger than anything that is not null).
	 */
	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return true;
	}

	/**
	 * Returns whether other deletes are visible.
	 */
	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * Returns whether other inserts are visible.
	 */
	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * Returns whether other updates are visible.
	 */
	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * Returns whether own deletes are visible
	 */
	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * Returns whether own inserts are visible
	 */
	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * Returns whether own updates are visible
	 */
	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * Checks if for CREATE TABLE Test(ID INT), getTables returns test as the
	 * table name.
	 *
	 * @return false
	 */
	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	/**
	 * Checks if for CREATE TABLE "Test"(ID INT), getTables returns test as the
	 * table name.
	 *
	 * @return false
	 */
	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	/**
	 * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
	 * table name
	 *
	 * @return false
	 */
	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	/**
	 * Checks if for CREATE TABLE "Test"(ID INT), getTables returns Test as the
	 * table name.
	 *
	 * @return true
	 */
	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	/**
	 * Checks if for CREATE TABLE Test(ID INT), getTables returns TEST as the
	 * table name.
	 *
	 * @return true
	 */
	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return true;
	}

	/**
	 * Checks if for CREATE TABLE "Test"(ID INT), getTables returns TEST as the
	 * table name.
	 *
	 * @return false
	 */
	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	/**
	 * Returns whether SQL-92 entry level grammar is supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return true;
	}

	/**
	 * Returns whether SQL-92 full level grammer is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	/**
	 * Returns whether SQL-92 intermediate level grammar is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	/**
	 * Returns whether batch updates is supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	/**
	 * Returns whether the catalog name in INSERT, UPDATE, DELETE is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	/**
	 * Returns whether column aliasing is supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	/**
	 * Returns whether CONVERT is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	/**
	 * Returns whether CONVERT is supported for one datatype to another.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return false;
	}

	/**
	 * Returns whether ODBC Core SQL grammar is supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	/**
	 * Returns whether data manipulation and CREATE/DROP is supported in
	 * transactions.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return false;
	}

	/**
	 * Returns whether only data manipulations are supported in transactions.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return true;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	/**
	 * @return true
	 */
	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	/**
	 * Checks whether a GROUP BY clause can use columns that are not in the
	 * SELECT clause, provided that it specifies all the columns in the SELECT
	 * clause.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	/**
	 * Returns whether GROUP BY is supported if the column is not in the SELECT
	 * list.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return true;
	}

	/**
	 * @return true
	 */
	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return true;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	/**
	 * Returns whether ODBC Minimum SQL grammar is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return false;
	}

	/**
	 * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
	 * table name.
	 *
	 * TODO case sensitiveに変更する。
	 */
	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	/**
	 * Checks if a table created with CREATE TABLE "Test"(ID INT) is a different
	 * table than a table created with CREATE TABLE TEST(ID INT).
	 *
	 * @return false
	 */
	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	/**
	 * Does the database support multiple open result sets.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return true;
	}

	/**
	 * Returns whether multiple result sets are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	/**
	 * Returns whether multiple transactions are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	/**
	 * Does the database support named parameters.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	/**
	 * Returns whether columns with NOT NULL are supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	/**
	 * Returns whether open result sets across commits are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	/**
	 * Returns whether open result sets across rollback are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	/**
	 * Returns whether open statements across commit are supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return true;
	}

	/**
	 * Returns whether open statements across rollback are supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return true;
	}

	/**
	 * Returns whether ORDER BY is supported if the column is not in the SELECT
	 * list.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	/**
	 * Returns whether outer joins are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return false;
	}

	/**
	 * Returns whether positioned deletes are supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return true;
	}

	/**
	 * Returns whether positioned updates are supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return true;
	}

	/**
	 * Returns whether a specific result set concurrency is supported.
	 */
	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		return type != ResultSet.TYPE_SCROLL_SENSITIVE;
	}

	/**
	 * Does this database supports a result set holdability.
	 *
	 * @return true if the holdability is ResultSet.CLOSE_CURSORS_AT_COMMIT
	 */
	@Override
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	/**
	 * Returns whether a specific result set type is supported.
	 */
	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type != ResultSet.TYPE_SCROLL_SENSITIVE;
	}

	/**
	 * Returns wheter savepoints is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	/**
	 * Returns whether the schema name in INSERT, UPDATE, DELETE is supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return true;
	}

	/**
	 * Returns whether the schema name in CREATE INDEX is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	/**
	 * @return false
	 */
	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	/**
	 * Does the database support statement pooling.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	/**
	 * Returns whether the database supports calling functions using the call syntax.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	/**
	 * Returns whether stored procedures are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	/**
	 * Returns whether subqueries (SELECT) in comparisons are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	/**
	 * Returns whether SELECT in EXISTS is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	/**
	 * Returns whether IN(SELECT...) is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return false;
	}

	/**
	 * Returns whether subqueries in quantified expression are supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	/**
	 * Returns whether table correlation names (table alias) are supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return true;
	}

	/**
	 * Returns whether a specific transaction isolation level.
	 *
	 * @return true, if level is TRANSACTION_READ_COMMITTED
	 */
	@Override
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		switch (level) {
		case Connection.TRANSACTION_READ_COMMITTED:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Returns whether transactions are supported.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsTransactions() throws SQLException {
		return true;
	}

	/**
	 * Returns whether UNION SELECT is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	/**
	 * Returns whether UNION ALL SELECT is supported.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	/**
	 * Returns whether updates are detected.
	 *
	 * @return false
	 */
	@Override
	public boolean updatesAreDetected(int i) throws SQLException {
		return false;
	}

	/**
	 * Checks if this database use one file per table.
	 *
	 * @return false
	 */
	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	/**
	 * Checks if this database store data in local files.
	 *
	 * @return true
	 */
	@Override
	public boolean usesLocalFiles() throws SQLException {
		return true;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "isWrapperFor")
				.getSQLException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "unwrap")
				.getSQLException();
	}
}
