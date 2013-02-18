package net.unit8.solr.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class UpdateExecTest {
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
	public void update() throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("UPDATE player SET player_name=?, position=? WHERE player_id = ?");
		Statement assertStatement = null;
		try {
			stmt.setString(1, "小早川毅彦");
			String[] position = {"一塁手"}; 
			stmt.setObject(2, position);
			stmt.setInt(3, 5);
			stmt.executeUpdate();
			
			conn.commit();
			
			assertStatement = conn.createStatement();
			ResultSet rs1 = assertStatement.executeQuery("SELECT * FROM player WHERE player_id=5");
			assertTrue("1件だけ取得できる", rs1.next());
			assertEquals("小早川毅彦", rs1.getString("player_name"));
			Array array = rs1.getArray("position");
			Object[] resultPosition = (Object[])array.getArray();
			assertEquals("一塁手", resultPosition[0]);
		} finally {
			stmt.close();
			if(assertStatement != null) {
				assertStatement.close();
			}
		}
	}
	
	@Test
	public void updateContainsMetachar() throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("UPDATE player SET player_name=? WHERE player_id = ?");
		Statement assertStatement = null;
		try {
			stmt.setString(1, "&;:?");
			stmt.setInt(2, 5);
			assertEquals("1件更新される", 1, stmt.executeUpdate());
			
			
			conn.commit();
			
			assertStatement = conn.createStatement();
			ResultSet rs1 = assertStatement.executeQuery("SELECT * FROM player WHERE player_id=5");
			assertTrue("1件だけ取得できる", rs1.next());
			assertEquals("&;:?", rs1.getString("player_name"));
		} finally {
			stmt.close();
			
			if(assertStatement != null) {
				assertStatement.close();
			}
			
			PreparedStatement undoStmt = conn.prepareStatement("UPDATE player SET player_name=? WHERE player_name=5");
			undoStmt.setString(1, "ランディバース");
			undoStmt.executeUpdate();
			conn.commit();
			undoStmt.close();
		}
		
	}
	
	@Test
	public void delete() throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			ResultSet rs1 = stmt.executeQuery("SELECT count(*) AS cnt FROM player WHERE player_id=5");
			rs1.next();
			assertEquals("バースがいる", 1, rs1.getInt("cnt"));
			stmt.executeUpdate("DELETE FROM player WHERE player_id = 5");
			conn.commit();
			ResultSet rs2 = stmt.executeQuery("SELECT count(*) AS cnt FROM player WHERE player_id=5");
			rs2.next();
			assertEquals("バースが消える", 0, rs2.getInt("cnt"));
		} finally {
			stmt.close();

			// 元に戻しておく
			PreparedStatement ps = conn.prepareStatement("INSERT INTO  player Values (?,?,?,?,?)");
			try {
				ps.setInt(1, 5);
				ps.setString(2, "阪神");
				ps.setString(3, "ランディーバース");
				ps.setObject(4, new String[]{"一塁手","外野手"});
				ps.setDate(5, new Date(System.currentTimeMillis()));
				ps.executeUpdate();
				conn.commit();
			} finally {
				ps.close();
			}
		}
	}
	
	@Test
	public void batchUpdate() throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(
				"UPDATE player SET player_name=? WHERE player_id=?");
		try {
			stmt.setString(1, "東出");
			stmt.setInt(2, 1);
			stmt.addBatch();
			stmt.setString(1, "梵");
			stmt.setInt(2, 2);
			stmt.addBatch();
			stmt.setString(1, "赤松");
			stmt.setInt(2, 3);
			stmt.addBatch();
			int[] result = stmt.executeBatch();
			conn.commit();
		} finally {
			stmt.close();
		}
		
		PreparedStatement assertStmt = conn.prepareStatement(
				"SELECT player_name FROM player WHERE player_id=?");
		try {
			ResultSet rs;
			assertStmt.setInt(1, 1);
			rs = assertStmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("東出",rs.getString("player_name"));
			assertStmt.setInt(1, 2);
			rs = assertStmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("梵",rs.getString("player_name"));
			assertStmt.setInt(1, 3);
			rs = assertStmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("赤松",rs.getString("player_name"));
		} finally {
			assertStmt.close();
		}
	}
	
	@BeforeClass
	public static void init() throws SQLException, ClassNotFoundException {
		Connection setUpConn = null;
		Class.forName(SolrDriver.class.getName());

		try {
			setUpConn = DriverManager.getConnection("jdbc:solr:s");
			PreparedStatement dropStmt = setUpConn.prepareStatement("DROP TABLE player");
			try {
				dropStmt.executeUpdate();
			} catch(SQLException ignore) {
			} finally {
				dropStmt.close();
			}
	
			PreparedStatement stmt = setUpConn.prepareStatement(
					"CREATE TABLE player (player_id number, team varchar(10), player_name varchar(50), position varchar(10) ARRAY, registered_at DATE)");
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}
	
			PreparedStatement insStmt = setUpConn.prepareStatement("INSERT INTO player Values (?,?,?,?,?)");
			try {
				insStmt.setInt(1, 1);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "高橋慶彦");
				insStmt.setObject(4, new String[]{"遊撃手"});
				insStmt.setDate(5, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();
		
				insStmt.setInt(1, 2);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "山崎隆造");
				insStmt.setObject(4, new String[]{"遊撃手","二塁手"});
				insStmt.setDate(5, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();
		
				insStmt.setInt(1, 3);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "衣笠祥雄");
				insStmt.setObject(4, new String[]{"一塁手","三塁手"});
				insStmt.setDate(5, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();
		
				insStmt.setInt(1, 4);
				insStmt.setString(2, "カープ");
				insStmt.setString(3, "山本浩二");
				insStmt.setObject(4, new String[]{"外野手"});
				insStmt.setDate(5, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();
		
				insStmt.setInt(1, 5);
				insStmt.setString(2, "阪神");
				insStmt.setString(3, "ランディーバース");
				insStmt.setObject(4, new String[]{"一塁手","外野手"});
				insStmt.setDate(5, new Date(System.currentTimeMillis()));
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
