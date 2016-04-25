package com.mcd.gdw.daas.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.math.BigDecimal;

public class RDBMS {
	
	public class DBException extends Exception {

		private static final long serialVersionUID = -9167598243378372526L;
		
		private int sqlErrorCode;
		private String sqlErrorMessage;
		private String exceptionMessage;

		public DBException(String exceptionMessage) {

			super(exceptionMessage);
			
			this.sqlErrorCode = 0;
			this.sqlErrorMessage = "";
			this.exceptionMessage = exceptionMessage;
			
		}
		
		public DBException(int sqlErrorCode
	                      ,String sqlErrorMessage
	                      ,String exceptionMessage) {

			super(exceptionMessage);

			this.sqlErrorCode = sqlErrorCode;
			this.sqlErrorMessage = sqlErrorMessage;
			this.exceptionMessage = exceptionMessage;

		}

		public int getSqlErrorCode() {

			return(this.sqlErrorCode);

		}

		public String getSqlErrorMessage() {

			return(this.sqlErrorMessage);

		}

		public String getExceptionMessage() {

			return(this.exceptionMessage);

		}
	}
	
	public class MultiPreparedStatementSql {
		
		public String multiPreparedStatmentId;
		public String multiPreparedStatmentSql;
		
		public MultiPreparedStatementSql(String multiPreparedStatmentId
				                        ,String multiPreparedStatmentSql) {
			
			this.multiPreparedStatmentId = multiPreparedStatmentId;
			this.multiPreparedStatmentSql = multiPreparedStatmentSql;
			
		}
		
		public MultiPreparedStatementSql(String multiPreparedStatmentId
				                        ,StringBuffer multiPreparedStatmentSql) {
			
			this.multiPreparedStatmentId = multiPreparedStatmentId;
			this.multiPreparedStatmentSql = multiPreparedStatmentSql.toString();
			
		}
	}
	private class MultiPreparedStatement {
	
		private PreparedStatement multiPrepStmt;
		private ParameterMetaData multiPrepStmtMetaData;
		private int multiPrepStmtMetaDataSize;
		private int multiBatchCount;
		private int multiTotalUpdCount;
		private int multiBatchSize;
		
		public MultiPreparedStatement(String preparedStatementSql
				                     ,Connection conn
				                     ,int batchSize) throws RDBMS.DBException {

			try {
				
				this.multiPrepStmt = conn.prepareStatement(preparedStatementSql);
				this.multiPrepStmtMetaData = this.multiPrepStmt.getParameterMetaData();
				this.multiPrepStmtMetaDataSize = this.multiPrepStmtMetaData.getParameterCount();
				this.multiBatchCount =0;
				this.multiTotalUpdCount =0;
				this.multiBatchSize = batchSize;
				
			} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
				throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
				
			} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
				throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
				
			} catch (Exception ex) {
				throw new RDBMS.DBException(ex.toString());
			}
			
		}
	}
	
	public enum ConnectionType { SQLServer, Teradata, Aster }
	
	private final static String[] JDBC_DRIVER = {"com.microsoft.sqlserver.jdbc.SQLServerDriver"
		                                        ,"com.teradata.jdbc.TeraDriver"
		                                        ,"com.asterdata.ncluster.Driver"};
	
	private final static String[] URL_PREFIX = {"jdbc:sqlserver://"
		                                       ,"jdbc:teradata://"
		                                       ,"jdbc:ncluster://"};
	
	private final static String[] URL_COMMON_PARMS = {";loginTimeout=15"
		                                             ,"/TMODE=ANSI,CHARSET=UTF8"
	                                                 ,":2406/beehive"};
		
	private final static int DEFAULT_MAX_BATCH_SIZE = 10000;
	
	private Connection conn = null;
	private Statement stmt = null;

	private ConnectionType connectionType;
	private int connectionIdx =0;
	private String serverName;
	private String userId;
	private String password;
	private int numberOfSessions=0;
	private boolean autoCommit = false;
	private PreparedStatement prepStmt;
	private ParameterMetaData prepStmtMetaData;
	
	private HashMap<String,RDBMS.MultiPreparedStatement> multiPrepItems;
	
	private int prepStmtMetaDataSize = 0;
	private int batchCount = 0;
	private int[] batchUpdCnt;
	private int totalUpdCount =0;
	private int batchSize = DEFAULT_MAX_BATCH_SIZE;

	public RDBMS(ConnectionType connectionType
	   	        ,String serverName
		        ,String userId
		        ,String password) throws RDBMS.DBException  {
		
		this(connectionType,serverName,userId,password,1);
	
	}

	public RDBMS(ConnectionType connectionType
			    ,String serverName
			    ,String userId
			    ,String password
			    ,int numberOfSessions) throws RDBMS.DBException {
		
		String additionalConnectionSettings = "";
		
		this.connectionType = connectionType;
		this.serverName = serverName;
		this.userId = userId;
		this.password = password;
		this.numberOfSessions = numberOfSessions;

		try {

			if ( this.connectionType == ConnectionType.Teradata ) {
				this.connectionIdx = 1;
				additionalConnectionSettings = ",SESSIONS=" + this.numberOfSessions;
			} else if (this.connectionType == ConnectionType.Aster ) {
				this.connectionIdx = 2;
			} else {
				this.connectionIdx = 0;
			}
			
			Class.forName(JDBC_DRIVER[this.connectionIdx]);

            
            if ( this.connectionType == ConnectionType.SQLServer ) {
            	Logger logger = Logger.getLogger(JDBC_DRIVER[this.connectionIdx]); 
            	logger.setLevel(Level.OFF );
            }

			
			conn = DriverManager.getConnection(URL_PREFIX[this.connectionIdx] + this.serverName + URL_COMMON_PARMS[this.connectionIdx] + additionalConnectionSettings,this.userId,this.password);
            
            stmt = conn.createStatement();
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
	}

	public void dispose() {
		
		try {
			conn.close();
			
		} catch (Exception ex) {
			
		}
		
	}
	public ConnectionType getConnectionType() {
		
		return(this.connectionType);
		
	}
	
	public String getServerName() {
		
		return(this.serverName);
		
	}
	
	public String getUserId() {
		
		return(this.userId);
		
	}

	public int getNumberOfSessions() {
		
		return(this.numberOfSessions);
		
	}
	
	public int getBatchSize() {
		
		return(this.numberOfSessions);
		
	}
	
	public void setBatchSize(int batchSize) throws RDBMS.DBException {
		
		if ( batchSize < 1 ) {
			throw new RDBMS.DBException("Batch Size must be > 0");
		} else {
			this.batchSize = batchSize;
		}
	}
	
	public boolean getAutoCommit() {
		
		return(this.autoCommit);
		
	}
	
	public void setAutoCommit(boolean autoCommit) throws RDBMS.DBException {
		
		try {
			this.autoCommit = autoCommit;
			conn.setAutoCommit(autoCommit);
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
	}
	
	public void commit() throws RDBMS.DBException {
		
		try {
			conn.commit();
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
	}

	public void rollback() throws RDBMS.DBException {
		
		try {
			conn.rollback();
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
	}
	
	public int executeUpdate(String sql) throws RDBMS.DBException {

		int rowCount = 0; 
		
		try {
			rowCount = stmt.executeUpdate(sql);

		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}

		return(rowCount);
	}
	
	public void setPreparedStatement(String preparedStatementSql) throws RDBMS.DBException {

		try {
			prepStmt = conn.prepareStatement(preparedStatementSql);
			prepStmtMetaData = prepStmt.getParameterMetaData();
			prepStmtMetaDataSize = prepStmtMetaData.getParameterCount();
			batchCount =0;
			totalUpdCount =0;

		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
	}
	
	public String describeBatch() throws RDBMS.DBException {

		String paramType;
		String retValue = "";
		
		try {
			
			for (int parmIdx=1; parmIdx <= prepStmtMetaDataSize; parmIdx++ ) {
				
				paramType = prepStmtMetaData.getParameterTypeName(parmIdx).toLowerCase();
				
				if ( retValue.length() > 0 ) {
					retValue += "\n";
				}
				
				retValue += parmIdx + ")" + paramType;
			}
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
		
		return(retValue);
	}
	
	public int addBatch(Object...fields) throws RDBMS.DBException {
		
		String paramType = null;
		int updCount = 0;		
		
		try {
			
			for (int parmIdx=1; parmIdx <= prepStmtMetaDataSize; parmIdx++ ) {
				
				paramType = prepStmtMetaData.getParameterTypeName(parmIdx).toLowerCase();
				
				if ( paramType.equals("char") || paramType.equals("varchar") || paramType.equals("nvarchar") || paramType.equals("sql_varchar")) {
					prepStmt.setString(parmIdx, (String)fields[parmIdx-1]);
						
				} else if ( paramType.equals("smallint") || paramType.equals("tinyint")) {
					if( fields[parmIdx-1] != null)
						prepStmt.setShort(parmIdx, (Short)fields[parmIdx-1]);
					else
						prepStmt.setNull(parmIdx,Types.SMALLINT);
					
  			    } else if ( paramType.equals("int") ) {
  			    	if(fields[parmIdx-1] != null)
  			    		prepStmt.setInt(parmIdx, (Integer)fields[parmIdx-1]);
  			    	else
  			    		prepStmt.setNull(parmIdx,Types.INTEGER);
				    
					
				} else if ( paramType.equals("float")) {
					if(fields[parmIdx-1] != null)
						prepStmt.setFloat(parmIdx, (Float)fields[parmIdx-1]);
					else
  			    		prepStmt.setNull(parmIdx,Types.FLOAT);
						
				} else if ( paramType.equals("decimal")) {
					if(fields[parmIdx-1] != null)
						prepStmt.setBigDecimal(parmIdx, (BigDecimal)fields[parmIdx-1]);
					else
  			    		prepStmt.setNull(parmIdx,Types.DECIMAL);
						
				} else if ( paramType.equals("date") ) {
					if ( fields[parmIdx-1] == null ) {
						prepStmt.setNull(parmIdx, java.sql.Types.DATE);
					} else {
						prepStmt.setDate(parmIdx, (java.sql.Date)fields[parmIdx-1]);
					}

				} else if ( paramType.equals("datetime")) {
					if(fields[parmIdx-1] != null)
						prepStmt.setTimestamp(parmIdx, (Timestamp)fields[parmIdx-1]);
					else
						prepStmt.setNull(parmIdx, java.sql.Types.TIMESTAMP);
					
				}
			}
				
			batchCount++;
			prepStmt.addBatch();
				
			if ( batchCount >= this.batchSize ) {
				batchCount = 0;

				batchUpdCnt = prepStmt.executeBatch();
				for ( int uptCntIdx=0; uptCntIdx < batchUpdCnt.length; uptCntIdx++ ) {
					updCount += batchUpdCnt[uptCntIdx];
				}
				totalUpdCount+= updCount;
			}
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			tSqlEx.printStackTrace();
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RDBMS.DBException(ex.toString());
		}
		
		return(updCount);
	}
	
	public int finalizeBatch() throws RDBMS.DBException {
		
		int updCount = 0;
		
		try {
			if ( batchCount > 0 ) {
				batchCount = 0;

				batchUpdCnt = prepStmt.executeBatch();
				for ( int uptCntIdx=0; uptCntIdx < batchUpdCnt.length; uptCntIdx++ ) {
					updCount += batchUpdCnt[uptCntIdx];
				}
				totalUpdCount+= updCount;
			}
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}

		return(totalUpdCount);
	}

	
	public boolean executeStatement(Object...fields) throws RDBMS.DBException {
		
		String paramType = null;
		boolean retSuccess = false;		
		
		try {
			
			for (int parmIdx=1; parmIdx <= prepStmtMetaDataSize; parmIdx++ ) {
				
				paramType = prepStmtMetaData.getParameterTypeName(parmIdx).toLowerCase();
				
				if ( paramType.equals("char") || paramType.equals("varchar") || paramType.equals("nvarchar") ) {
					prepStmt.setString(parmIdx, (String)fields[parmIdx-1]);
						
				} else if ( paramType.equals("smallint") || paramType.equals("tinyint")) {
					if( fields[parmIdx-1] != null)
						prepStmt.setShort(parmIdx, (Short)fields[parmIdx-1]);
					else
						prepStmt.setNull(parmIdx,Types.SMALLINT);
					
  			    } else if ( paramType.equals("int") ) {
  			    	if(fields[parmIdx-1] != null)
  			    		prepStmt.setInt(parmIdx, (Integer)fields[parmIdx-1]);
  			    	else
  			    		prepStmt.setNull(parmIdx,Types.INTEGER);
				    
					
				} else if ( paramType.equals("float")) {
					if(fields[parmIdx-1] != null)
						prepStmt.setFloat(parmIdx, (Float)fields[parmIdx-1]);
					else
  			    		prepStmt.setNull(parmIdx,Types.FLOAT);
						
				} else if ( paramType.equals("decimal")) {
					if(fields[parmIdx-1] != null)
						prepStmt.setBigDecimal(parmIdx, (BigDecimal)fields[parmIdx-1]);
					else
  			    		prepStmt.setNull(parmIdx,Types.DECIMAL);
						
				} else if ( paramType.equals("date") ) {
					if ( fields[parmIdx-1] == null ) {
						prepStmt.setNull(parmIdx, java.sql.Types.DATE);
					} else {
						prepStmt.setDate(parmIdx, (java.sql.Date)fields[parmIdx-1]);
					}

				} else if ( paramType.equals("datetime")) {
					if(fields[parmIdx-1] != null)
						prepStmt.setTimestamp(parmIdx, (Timestamp)fields[parmIdx-1]);
					else
						prepStmt.setNull(parmIdx, java.sql.Types.TIMESTAMP);
					
				}
			}

			retSuccess = prepStmt.execute();
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			tSqlEx.printStackTrace();
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RDBMS.DBException(ex.toString());
		}
		
		return(retSuccess);
	}

	
	
	
	public void multiSetPreparedStatement(Object...sql) throws Exception {
	
		multiPrepItems = new HashMap<String,RDBMS.MultiPreparedStatement>();
		
		for (int idx=0; idx < sql.length; idx++ ) {
			if ( sql[idx] instanceof MultiPreparedStatementSql ) {
				MultiPreparedStatementSql newItem = (MultiPreparedStatementSql)sql[idx];
				MultiPreparedStatement newPrepItem = new MultiPreparedStatement(newItem.multiPreparedStatmentSql,this.conn,this.batchSize);
				
				multiPrepItems.put(newItem.multiPreparedStatmentId, newPrepItem);
				
			} else { 
				throw new Exception("Object is not RDBMS.MultiPreparedStatementSql object");
			}
		}
		
	}
	
	public int multiAddBatch(String prepItemKey
			                ,Object...fields) throws RDBMS.DBException {
		
		String paramType = null;
		int updCount = 0;		
		MultiPreparedStatement prepItem;
		
		try {
			
			prepItem = multiPrepItems.get(prepItemKey);
			
			if ( prepItem.multiPrepStmtMetaDataSize == 0 ) {
				for (int parmIdx=1; parmIdx <= fields.length; parmIdx++) {
					if ( fields[parmIdx-1] instanceof String ) {
						prepItem.multiPrepStmt.setString(parmIdx, (String)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof Short ) {
						prepItem.multiPrepStmt.setShort(parmIdx, (Short)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof Integer ) {
						prepItem.multiPrepStmt.setInt(parmIdx, (Integer)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof Float ) {
						prepItem.multiPrepStmt.setFloat(parmIdx, (Float)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof BigDecimal ) {
						prepItem.multiPrepStmt.setBigDecimal(parmIdx, (BigDecimal)fields[parmIdx-1]);
					}
				}
			}
			
			for (int parmIdx=1; parmIdx <= prepItem.multiPrepStmtMetaDataSize; parmIdx++ ) {
				
				paramType = prepItem.multiPrepStmtMetaData.getParameterTypeName(parmIdx).toLowerCase();
				
				if ( paramType.equals("char") || paramType.equals("varchar") || paramType.equals("nvarchar") ) {
					prepItem.multiPrepStmt.setString(parmIdx, (String)fields[parmIdx-1]);
						
				} else if ( paramType.equals("smallint") || paramType.equals("tinyint")) {
					prepItem.multiPrepStmt.setShort(parmIdx, (Short)fields[parmIdx-1]);
					
  			    } else if ( paramType.equals("int") ) {
  			    	prepItem.multiPrepStmt.setInt(parmIdx, (Integer)fields[parmIdx-1]);
					
				} else if ( paramType.equals("float")) {
					prepItem.multiPrepStmt.setFloat(parmIdx, (Float)fields[parmIdx-1]);
						
				} else if ( paramType.equals("decimal")) {
					prepItem.multiPrepStmt.setBigDecimal(parmIdx, (BigDecimal)fields[parmIdx-1]);
						
				} else if ( paramType.equals("date") ) {
					if ( fields[parmIdx-1] == null ) {
						prepItem.multiPrepStmt.setNull(parmIdx, java.sql.Types.DATE);
					} else {
						prepItem.multiPrepStmt.setDate(parmIdx, (java.sql.Date)fields[parmIdx-1]);
					}

				} else if ( paramType.equals("datetime")) {
					prepItem.multiPrepStmt.setTimestamp(parmIdx, (Timestamp)fields[parmIdx-1]);
					
				}else if ( paramType.equals("bigint")) {
					prepItem.multiPrepStmt.setFloat(parmIdx, (Float)fields[parmIdx-1]);
						
				}
			}
				
			prepItem.multiBatchCount++;
			prepItem.multiPrepStmt.addBatch();
				
			if ( prepItem.multiBatchCount >= prepItem.multiBatchSize ) {
				prepItem.multiBatchCount = 0;

				batchUpdCnt = prepItem.multiPrepStmt.executeBatch();
				for ( int uptCntIdx=0; uptCntIdx < batchUpdCnt.length; uptCntIdx++ ) {
					updCount += batchUpdCnt[uptCntIdx];
				}
				prepItem.multiTotalUpdCount+= updCount;
			}
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			tSqlEx.printStackTrace();
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
		
		return(updCount);
	}
	
	public int multiFinalizeBatch(String prepItemKey) throws RDBMS.DBException {
		
		int updCount = 0;
		MultiPreparedStatement prepItem;
		
		try {
			
			prepItem = multiPrepItems.get(prepItemKey);

			if ( prepItem.multiBatchCount > 0 ) {
				prepItem.multiBatchCount = 0;

				batchUpdCnt = prepItem.multiPrepStmt.executeBatch();
				for ( int uptCntIdx=0; uptCntIdx < batchUpdCnt.length; uptCntIdx++ ) {
					updCount += batchUpdCnt[uptCntIdx];
				}
				prepItem.multiTotalUpdCount+= updCount;
			}
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}

		return(prepItem.multiTotalUpdCount);
	}
	
	public boolean multiExecute(String prepItemKey
			               ,Object...fields) throws RDBMS.DBException {
		
		String paramType = null;
		MultiPreparedStatement prepItem;
		boolean retSuccess = false;		
		
		try {
			
			prepItem = multiPrepItems.get(prepItemKey);
			
			if ( prepItem.multiPrepStmtMetaDataSize == 0 ) {
				for (int parmIdx=1; parmIdx <= fields.length; parmIdx++) {
					if ( fields[parmIdx-1] instanceof String ) {
						prepItem.multiPrepStmt.setString(parmIdx, (String)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof Short ) {
						prepItem.multiPrepStmt.setShort(parmIdx, (Short)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof Integer ) {
						prepItem.multiPrepStmt.setInt(parmIdx, (Integer)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof Float ) {
						prepItem.multiPrepStmt.setFloat(parmIdx, (Float)fields[parmIdx-1]);
					} else if ( fields[parmIdx-1] instanceof BigDecimal ) {
						prepItem.multiPrepStmt.setBigDecimal(parmIdx, (BigDecimal)fields[parmIdx-1]);
					}
				}
			}
			
			for (int parmIdx=1; parmIdx <= prepItem.multiPrepStmtMetaDataSize; parmIdx++ ) {
				
				paramType = prepItem.multiPrepStmtMetaData.getParameterTypeName(parmIdx).toLowerCase();
				
				if ( paramType.equals("char") || paramType.equals("varchar") || paramType.equals("nvarchar") ) {
					prepItem.multiPrepStmt.setString(parmIdx, (String)fields[parmIdx-1]);
						
				} else if ( paramType.equals("smallint") || paramType.equals("tinyint")) {
					prepItem.multiPrepStmt.setShort(parmIdx, (Short)fields[parmIdx-1]);
					
  			    } else if ( paramType.equals("int") ) {
  			    	prepItem.multiPrepStmt.setInt(parmIdx, (Integer)fields[parmIdx-1]);
					
				} else if ( paramType.equals("float")) {
					prepItem.multiPrepStmt.setFloat(parmIdx, (Float)fields[parmIdx-1]);
						
				} else if ( paramType.equals("decimal")) {
					prepItem.multiPrepStmt.setBigDecimal(parmIdx, (BigDecimal)fields[parmIdx-1]);
						
				} else if ( paramType.equals("date") ) {
					if ( fields[parmIdx-1] == null ) {
						prepItem.multiPrepStmt.setNull(parmIdx, java.sql.Types.DATE);
					} else {
						prepItem.multiPrepStmt.setDate(parmIdx, (java.sql.Date)fields[parmIdx-1]);
					}

				} else if ( paramType.equals("datetime")) {
					prepItem.multiPrepStmt.setTimestamp(parmIdx, (Timestamp)fields[parmIdx-1]);
					
				}
			}
				
			retSuccess = prepItem.multiPrepStmt.execute();
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
		
		return(retSuccess);
	}
	
	public ResultSet resultSet(String sql) throws RDBMS.DBException {

		ResultSet retResults = null; 

		try {
			retResults = stmt.executeQuery(sql);
			
		} catch (com.microsoft.sqlserver.jdbc.SQLServerException tSqlEx) {
			throw new RDBMS.DBException(tSqlEx.getErrorCode(), tSqlEx.getMessage(), tSqlEx.toString());
			
		} catch (com.teradata.jdbc.jdbc_4.util.JDBCException teradataSqlEx) {
			throw new RDBMS.DBException(teradataSqlEx.getErrorCode(),teradataSqlEx.getMessage(),teradataSqlEx.toString());
			
		} catch (Exception ex) {
			throw new RDBMS.DBException(ex.toString());
		}
			
		return(retResults);
	
	}
}