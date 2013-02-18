package net.unit8.solr.jdbc;

import net.unit8.solr.jdbc.message.ErrorCode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class SelectQueryTest {
	Connection conn;

	@Before
	public void setUp() throws Exception {
		conn = DriverManager.getConnection("jdbc:solr:s;SOLR_HOME=src/test/resources");
	}

	@After
	public void tearDown() throws Exception {
		conn.close();
	}

	@Test
	public void testStatement() throws SQLException {
		String[][] expected = {{"高橋慶彦"},{"山崎隆造"},{"衣笠祥雄"},{"山本浩二"},{"ランディーバース"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id",
				expected);
	}

	@Test
	public void testStatementLimit() throws SQLException {
		String[][] expected = {{"山崎隆造"},{"衣笠祥雄"},{"山本浩二"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id LIMIT 3 OFFSET 1",
				expected);
	}

	@Test
	public void testStatementOrderBy() throws SQLException {
		Object[][] expected = {{"ランディーバース"},{"山本浩二"},{"衣笠祥雄"},{"山崎隆造"},{"高橋慶彦"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id DESC",
				expected);
	}

	@Test
	public void testStatementCondition() throws SQLException {
		Object[][] expected1 = {{"山崎隆造"}};
		verifyStatement("SELECT player_name FROM player WHERE player_id > 1 AND player_id < 3",
				expected1);
		Object[][] expected2 = {{"高橋慶彦"}, {"山崎隆造"}, {"衣笠祥雄"}};
		verifyStatement("SELECT player_name FROM player WHERE player_id >= 1 AND player_id <= 3",
				expected2);
	}

	@Test
	public void testStatementOr() throws SQLException {
		Object[][] expected1 = {{"ランディーバース"}};
		Object[] params = {"阪神"};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE (player_id = 1 OR player_id = 5) AND team=?",
				params,
				expected1);
	}

	@Test
	public void testStatementCount() throws SQLException {
		Object[][] expected = {{"5"}};
		verifyStatement("SELECT count(*) FROM player", expected);
	}

	@Test
	public void testStatementGroupBy() throws SQLException {
		Object[][] expected = {{"カープ", "4"}, {"阪神", "1"}};
		verifyStatement("SELECT team, count(*) FROM player GROUP BY team", expected);
	}

	@Test
	public void testStatementIn() throws SQLException {
		Object[][] expected = {{"山崎隆造"}, {"衣笠祥雄"}, {"ランディーバース"}};
		Object[] params = {"一塁手", "二塁手"};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE position in (?,?) order by player_id",
				params,
				expected);
	}

	@Test
	public void testLike() throws SQLException {
		Object[][] expected = {{"衣笠祥雄"}};
		Object[] params = {"衣笠%"};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE player_name like ?",
				params,
				expected);
	}

	@Test
	public void testLikeForText() throws SQLException {
		Object[][] expected = {{"高橋慶彦"}};
		Object[] params = {"%三拍子%"};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE comment like ?",
				params,
				expected);
	}

	@Test
	public void testBetween() throws SQLException {
		Object[][] expected = {{"山崎隆造"}, {"衣笠祥雄"}};
		Object[] params = {2,3};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE player_id BETWEEN ? AND ?",
				params,
				expected);
	}

	@Test
	public void testBoolean() throws SQLException {
		Object[][] expected = {{"山本浩二"}, {"ランディーバース"}};
		Object[] params = {true};
		verifyPreparedStatement("SELECT player_name FROM player WHERE was_homerun_king=?",
				params, expected);
	}

	/**
	 * メタキャラクタを含んだクエリのテスト
	 * エラーにならないこと
	 *
	 * @throws SQLException
	 */
	@Test
	public void testQueryContainsMetachar() throws SQLException {
		Object[][] expected = {};
		Object[] params = {";&?"};

		verifyPreparedStatement("SELECT player_name FROM player WHERE player_name=?",
				params, expected);
	}

	@Test
	public void testStatementTableNotFound() {
		try {
			conn.prepareStatement("select * from prayer");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("TableOrViewNotFound", ErrorCode.TABLE_OR_VIEW_NOT_FOUND, e.getErrorCode());
		}

	}


	@Test
	public void testStatementColumnNotFound() throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("select prayer_name from player");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("ColumnNotFound", ErrorCode.COLUMN_NOT_FOUND, e.getErrorCode());
		} finally {
			if(stmt != null)
				stmt.close();
		}
	}

	/**
	 * get resultSet by column name
	 */
	@Test
	public void testGetColumnLabel() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("Select player_name from player where player_id=3");
			assertTrue(rs.next());
			assertEquals("player_name", rs.getMetaData().getColumnLabel(1));
			assertEquals("衣笠祥雄", rs.getString("player_name"));
		} finally {
            if (stmt != null)
                stmt.close();
		}
	}

	private void verifyStatement(String selectQuery, Object[][] expected) throws SQLException{
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			int i=0;
			while(rs.next()) {
				for(int j=0; j<expected[i].length; j++) {
					assertEquals(expected[i][j], rs.getString(j+1));
				}
				i+=1;
			}
			assertEquals("件数が正しい", expected.length, i);
		} catch(SQLException e) {
			e.printStackTrace();
			fail("SQLException:" + e.getMessage());
		} finally {
            if (stmt != null) {
                stmt.close();
            }
        }
	}

	private void verifyPreparedStatement(String selectQuery, Object[] params, Object[][] expected)
		throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(selectQuery);
			for(int i=0; i<params.length; i++) {
				stmt.setObject(i+1, params[i]);
			}
			ResultSet rs = stmt.executeQuery();
			int i=0;
			while(rs.next()) {
				for(int j=0; j<expected[i].length; j++) {
					assertEquals(expected[i][j], rs.getString(j+1));
				}
				i+=1;
			}
			assertEquals("件数が正しい", expected.length, i);
		} catch(SQLException e) {
			e.printStackTrace();
			fail("SQLException:" + e.getMessage());
		} finally {
			if(stmt != null)
				stmt.close();
		}

	}

	@BeforeClass
	public static void init() throws SQLException, ClassNotFoundException {
		Class.forName(SolrDriver.class.getName());
		Connection setUpConn = DriverManager.getConnection("jdbc:solr:s");

		try {
			PreparedStatement dropStmt = setUpConn.prepareStatement("DROP TABLE player");
			try {
				dropStmt.executeUpdate();
			} catch(SQLException ignore) {
				ignore.printStackTrace();
			} finally {
				dropStmt.close();
			}

			PreparedStatement stmt = setUpConn.prepareStatement(
					"CREATE TABLE player (player_id number, team varchar(10), "
					+ " player_name varchar(50), position varchar(10) ARRAY, "
					+ " was_homerun_king boolean, comment TEXT,"
					+ " registered_at DATE)");
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}

			PreparedStatement insStmt = setUpConn.prepareStatement("INSERT INTO player Values (?,?,?,?,?,?,?)");
			try {
				insStmt.setInt(1, 1);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "高橋慶彦");
				insStmt.setObject(4, new String[]{"遊撃手"});
				insStmt.setBoolean(5, false);
				insStmt.setString(6, "走攻守の三拍子そろった切込隊長");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 2);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "山崎隆造");
				insStmt.setObject(4, new String[]{"遊撃手","二塁手"});
				insStmt.setBoolean(5, false);
				insStmt.setString(6, "ベストナイン3回、ゴールデングラブ賞4回");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 3);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "衣笠祥雄");
				insStmt.setObject(4, new String[]{"一塁手","三塁手"});
				insStmt.setBoolean(5, false);
				insStmt.setString(6, "鉄人。国民栄誉賞");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 4);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "山本浩二");
				insStmt.setObject(4, new String[]{"外野手"});
				insStmt.setBoolean(5, true);
				insStmt.setString(6, "ミスター赤ヘル");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 5);
				insStmt.setString(2, "阪神");
				insStmt.setString(3, "ランディーバース");
				insStmt.setObject(4, new String[]{"一塁手","外野手"});
				insStmt.setBoolean(5, true);
				insStmt.setString(6, "三冠王");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();
			} finally {
				insStmt.close();
			}

			setUpConn.commit();
		} finally {
			setUpConn.close();
		}
	}
}
