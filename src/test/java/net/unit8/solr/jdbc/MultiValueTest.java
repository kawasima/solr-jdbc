package net.unit8.solr.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import java.sql.*;

public class MultiValueTest {
	Connection conn;

	@BeforeClass
	public static void setUpLog() {
		Logger logger = org.slf4j.LoggerFactory.getLogger("");
	}
	@Before
	public void setUp() throws Exception{
		Class.forName(SolrDriver.class.getName());

		conn = DriverManager.getConnection("jdbc:solr:s;SOLR_HOME=src/test/resources");
		PreparedStatement dropStmt = conn.prepareStatement("DROP TABLE books");
		try {
			dropStmt.executeUpdate();
		} catch(Exception ignore) {
		}

		PreparedStatement stmt = conn.prepareStatement(
				"CREATE TABLE books (title varchar(50), author varchar(50) ARRAY )");
		stmt.executeUpdate();

        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, "books", "%");
        while(rs.next()) {
            System.out.println(rs.getString("COLUMN_NAME") + ":" +
                    rs.getString("TYPE_NAME"));
        }
		PreparedStatement insStmt = conn.prepareStatement("INSERT INTO books Values (?,?)");
		insStmt.setString(1, "バカの壁");
		insStmt.setObject(2, new String[]{"養老孟司"});
		insStmt.executeUpdate();

		insStmt.setString(1, "勝間さん、努力で幸せになれますか");
		insStmt.setObject(2, new String[]{"勝間和代", "香山リカ"});
		insStmt.executeUpdate();

		conn.commit();
	}

    @After
    public void tearDown() throws SQLException {
        if (!conn.isClosed())
            conn.close();
    }

	@Test
	public void multiValue() throws Exception {
		PreparedStatement selStmt = conn.prepareStatement(
				"SELECT * FROM books");
		ResultSet rs = selStmt.executeQuery();
		while(rs.next()) {
			System.out.println(rs.getString("title"));
			System.out.println(rs.getString("author"));
		}
	}

	@Test
	public void multiValueWhereClause() throws Exception {
		PreparedStatement selStmt = conn.prepareStatement(
				"SELECT * FROM books where author = ?");
		selStmt.setString(1, "勝間和代");
		ResultSet rs = selStmt.executeQuery();
		while(rs.next()) {
			System.out.println(rs.getString("title"));
			System.out.println(rs.getString("author"));
			Object[] authors = (Object[])rs.getArray("author").getArray();
			for(Object author : authors) {
				System.out.println(author);
			}
		}
	}

}
