package com.hangum.tadpole.hive.core.connections;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hive.jdbc.HiveConnection;
import org.apache.log4j.Logger;

import com.hangum.tadpole.commons.libs.core.define.PublicTadpoleDefine.SQL_STATEMENT_TYPE;
import com.hangum.tadpole.engine.query.dao.mysql.TableColumnDAO;
import com.hangum.tadpole.engine.query.dao.mysql.TableDAO;
import com.hangum.tadpole.engine.query.dao.system.UserDBDAO;
import com.hangum.tadpole.engine.query.surface.ConnectionInterfact;
import com.hangum.tadpole.engine.sql.util.resultset.QueryExecuteResultDTO;

public class HiveConnectionManager implements ConnectionInterfact {
	static {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static final Logger logger = Logger.getLogger(HiveConnectionManager.class);
	private static Map<String, String> tableDataSourceMap = new HashMap();

	public static Connection getInstance(final UserDBDAO userDB) throws Exception {
		//java.sql.Connection javaConn = new HiveConnection(userDB.getUrl(), new java.util.Properties());
		java.sql.Connection javaConn = DriverManager.getConnection(userDB.getUrl(), userDB.getUsers(), userDB.getPasswd());
			
		return javaConn;
	}
	
	public static String getKeyworkd(final UserDBDAO userDB) throws Exception {
		return "DATASOURCE DATASOURCES REGISTER SHOW DESCRIBE";
		
	}

	@Override
	public void executeUpdate(UserDBDAO userDB, String sqlQuery) throws Exception {
		throw new RuntimeException("Update is not supported..");
	}

	@Override
	public QueryExecuteResultDTO select(UserDBDAO userDB, String requestQuery,
			Object[] statementParameter, int queryResultCount) throws Exception {
		if(logger.isDebugEnabled()) logger.debug("\t * Query is [ " + requestQuery );
		
		java.sql.Connection javaConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			javaConn = getInstance(userDB);
			pstmt = javaConn.prepareStatement(requestQuery);
			if(statementParameter != null) {
				for (int i=1; i<=statementParameter.length; i++) {
					pstmt.setObject(i, statementParameter[i-1]);					
				}
			}
			rs = pstmt.executeQuery();
			
			return new QueryExecuteResultDTO(userDB, requestQuery, true, rs, queryResultCount);
		} catch(Exception e) {
			logger.error("Hive select", e);
			throw e;
			
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
			try { if(rs != null) rs.close(); } catch(Exception e) {}
			try { if(javaConn != null) javaConn.close(); } catch(Exception e){}
		}
	}

	@Override
	public void connectionCheck(UserDBDAO userDB) throws Exception {
		Connection conn = null;
		ResultSet rs = null;
		
		try {
			conn = getInstance(userDB);
			DatabaseMetaData dbmd = conn.getMetaData();
	    	rs = dbmd.getTables(null, null, null, null);
	    	
		} catch(Exception e) {
			logger.error("connection check", e);
			throw e;
		} finally {
			try { if(rs != null) rs.close(); } catch(Exception e) {}
			try { if(conn != null) conn.close(); } catch(Exception e) {}
		}
	}

	@Override
	public List<TableDAO> tableList(UserDBDAO userDB) throws Exception {
		List<TableDAO> showTables = new ArrayList<TableDAO>();
		
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		
		try {
			conn = getInstance(userDB);
			st = conn.createStatement(); 
	    	rs = st.executeQuery("SHOW TABLES");

	    	while(rs.next()) {
	    		String strTBName = rs.getString("TABLE");
	    		Boolean temporary = rs.getBoolean("Is Temporary");
	    		String dataSource = rs.getString("Data Source");
	    		
	    		TableDAO tdao = new TableDAO(strTBName, "Data Source : " + dataSource);
	    		tdao.setTable_type(temporary? "Temporary" : "Non temporary");
	    		showTables.add(tdao);
	    		
	    		tableDataSourceMap.put(strTBName, dataSource);
	    	}
	    	
		} catch(Exception e) {
			logger.error("table list", e);
			throw e;
		} finally {
			try { if(rs != null) rs.close(); } catch(Exception e) {}
			try { if(st != null) st.close(); } catch(Exception e) {}
			try { if(conn != null) conn.close(); } catch(Exception e) {}
		}
		
		return showTables;
	}

	@Override
	public List<TableColumnDAO> tableColumnList(UserDBDAO userDB,
			Map<String, String> mapParam) throws Exception {
		List<TableColumnDAO> showTableColumns = new ArrayList<TableColumnDAO>();
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		
		try {
			conn = getInstance(userDB);
			st = conn.createStatement();
			String dataSource = tableDataSourceMap.get(mapParam.get("table"));
	    	rs = st.executeQuery("DESC " + (dataSource == null ? "" : dataSource) + "." + mapParam.get("table"));

	    	while(rs.next()) {
	    		TableColumnDAO tcDAO = new TableColumnDAO();
	    		tcDAO.setName(rs.getString("col_name"));
	    		tcDAO.setType(rs.getString("data_type"));
	    		
	    		tcDAO.setComment(rs.getString("comment"));
	    		
	    		showTableColumns.add(tcDAO);
	    	}
	    	
		} catch(Exception e) {
			logger.error(mapParam.get("table") + " table column", e);
			throw e;
		} finally {
			try { if(rs != null) rs.close(); } catch(Exception e) {}
			try { if(st != null) st.close(); } catch(Exception e) {}
			try { if(conn != null) conn.close(); } catch(Exception e) {}
		}
		
		return showTableColumns;
	}
	
	public QueryExecuteResultDTO executeQueryPlan(UserDBDAO userDB, String strQuery, SQL_STATEMENT_TYPE sql_STATEMENT_TYPE, Object[] statementParameter) throws Exception {
		return select(userDB, "EXPLAIN " + strQuery, null, 1000);
	}


}
