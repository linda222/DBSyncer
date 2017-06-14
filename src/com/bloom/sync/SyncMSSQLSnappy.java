package com.bloom.sync;

import java.math.BigDecimal;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.bloom.dbsync.db.MSSQLDataStore;
import com.bloom.dbsync.db.SnappyDataStore;
import com.bloom.dbsync.fileLogger.LogWriter;
import com.bloom.dbsync.logreader.MSSQLRepObjects;
public class SyncMSSQLSnappy extends SyncData {
	private ArrayList<syncObjects> objectsToSync;
	private String whereIncludeSchemas;
	private String whereExcludeSchemas;
	private String sourceHost;
	private String sourcePort;
	private String sourceUser;
	private String sourcePass;
	private String sourceServiceName;
	private String sourceUrl;
	private String targetHost;
	private String targetPort;
	private String targetPass;
	private String targetUser;
	private String targetServiceName;
	private MSSQLDataStore sourceDB;
	private static SnappyDataStore snappyDB;

	private String supportedDataTypes;
	private int batchSize;
	private int fullSyncloggingBatchSize; // 完全同步日志批量大小
	private String fullSyncTargetObjectExistsAction;
	private MSSQLRepObjects databasesAndObjects;
	private int tableRecordsCount = 0;
	private long tableSyncStartTime;
	private long tableSyncEndTime;
	private long batchT1;
	private long batchT2 = 0L;
	private String configurationName;
	private Boolean convertDataToString;
	private java.sql.Connection snappyConn;
	private String dateFormat;
	private String dateFormatNoTZ;
	private String tzOffset;
	private int parallelism;
	private SimpleDateFormat outputDateFormat;
	
	private static final String snappyDriverName = "io.snappydata.jdbc.ClientDriver";

	public SyncMSSQLSnappy(String configName) throws Exception {
		super(configName);
		this.configurationName = configName;
		this.logger = new LogWriter(SyncMSSQLSnappy.class);

		Properties pro = new Properties();
		pro.put("sourceHost", "192.168.16.103");
		pro.put("sourcePort", "1433");
		pro.put("sourceUser", "sa");
		pro.put("sourcePassword", "123456");
		pro.put("sourceServiceName", "DBSyncer");
		pro.put("targetHost", "192.168.15.121");
		pro.put("targetPort", "1527");
		pro.put("targetUser", "");
		pro.put("targetPassword", "");
		pro.put("targetServiceName", "");

		pro.put("tzOffset", "+00:00");
		pro.put("fullSyncBatchSize", "1000");
		pro.put("fullSyncLoggingBatch", "10000");
		pro.put("fullSyncLoggingBatchSize", "10000");

		pro.put("fullSyncTargetObjectExistsAction", "drop");

		pro.put("dateTimeFormat", "yyyy/MM/dd HH:mm:ss");
		pro.put("parallelism", 4);

		this.sourceHost = pro.get("sourceHost").toString();
		this.sourcePort = pro.get("sourcePort").toString();
		this.sourceUser = pro.get("sourceUser").toString();
		this.sourcePass = pro.get("sourcePassword").toString();
		this.sourceServiceName = pro.get("sourceServiceName").toString();
		this.sourceUrl = "jdbc:sqlserver://" + this.sourceHost + ":" + this.sourcePort + ";databaseName="
				+ this.sourceServiceName + ";user=" + this.sourceUser + ";password=" + this.sourcePass;

		this.targetHost = pro.get("targetHost").toString();
		this.targetPort = pro.get("targetPort").toString();
		this.targetUser = pro.get("targetUser").toString();
		this.targetPass = pro.get("targetPassword").toString();
		this.targetServiceName = pro.get("targetServiceName").toString();

		this.objectsToSync = new ArrayList();

		this.tzOffset = pro.get("tzOffset").toString();
		if (this.tzOffset.equals("")) {
			this.tzOffset = pro.get("tzOffset").toString();

			if (this.tzOffset.equals("")) {
				this.tzOffset = "+00:00";
				this.logger.logClass.warn("Can not get source tzOffset, it is set to +00:00");
			}

		}

		this.batchSize = Integer.parseInt(pro.get("fullSyncBatchSize").toString());
		this.fullSyncloggingBatchSize = Integer.parseInt(pro.get("fullSyncBatchSize").toString()) * 10;

		this.dateFormatNoTZ = pro.get("dateTimeFormat").toString();
		this.dateFormat = (this.dateFormatNoTZ + "X");

		this.parallelism = Integer.parseInt(pro.get("parallelism").toString());
		this.outputDateFormat = new SimpleDateFormat(this.dateFormat);

		this.fullSyncTargetObjectExistsAction = pro.get("fullSyncTargetObjectExistsAction").toString();
		try {
			connectToSource();
		} catch (SQLException sqlEx) {
			this.logger.logClass.error("连接到源数据库失败, 错误为 [" + sqlEx.getMessage() + "]");
			throw sqlEx;
		}
		// 连接 MSSQL 源数据库
		try {
			this.databasesAndObjects = new MSSQLRepObjects(this.configurationName, this.sourceDB);
		} catch (Exception ex) {
			this.logger.logClass.error("不能从数据源创建词典, 错误信息为:" + ex.getMessage());
			throw ex;
		}
		try {
			// 获得需要同步的数据库和表的列表
			getObjectsList();

		} catch (SQLException sqlEx) {
			this.logger.logClass.error("处理对象列表发生错误 ");
			throw sqlEx;
		}
	}

	public void connectToSource() throws SQLException {
		String url = "jdbc:sqlserver://" + this.sourceHost + ":" + this.sourcePort + ";databaseName="
				+ this.sourceServiceName + ";user=" + this.sourceUser + ";password=" + this.sourcePass;
		this.logger.logClass.debug("Try connecting to Source sqlserver Database");
		this.logger.logClass.debug(new StringBuilder().append("Connection URL=").append(url).toString());
		this.sourceDB = new MSSQLDataStore("syncsqlserverSnappy", url);
	}

	public void connectToTarget() throws Exception {
		Class.forName(snappyDriverName);

		String snappyConnectionUrl = new StringBuilder().append("jdbc:snappydata:thrift://").append(this.targetHost)
				.append("[").append(this.targetPort).append("]").toString();
		snappyDB = new SnappyDataStore("MSSQLReader", snappyConnectionUrl);
	}

	public void mssqlFullSyncEstimate() {
		boolean connected = false;
		int totalRows = 0;
		int syncCompleteTime = totalRows / this.plugNumber;
		this.logger.logClass.info("完全同步评估已经开始!");
		String query = "";
		String dbName = "";
		try {
			connectToSource();
			connected = true;
		} catch (Exception ex) {
			this.logger.logClass.error("连接到源数据库失败, 错误为 [" + ex.getMessage() + "]");
		}
		if (connected) {
			try {
				for (int i = 0; i < this.databasesAndObjects.getSize(); i++) {
					// 获得要同步的数据库
					dbName = this.databasesAndObjects.getDbNameByIndex(i);
					// 得到要同步的数据库所有表的总行数
					query = "select sum(i.rows)  from " + dbName + ".sys.tables t, " + dbName + ".sys.sysindexes i "
							+ " where t.type = 'U' AND i.id = t.object_id AND i.indid in (0,1) AND t.is_ms_shipped=0 ";

					Statement stmt = this.sourceDB.getMSSQLConnection().createStatement();
					ResultSet rset = stmt.executeQuery(query);
					while (rset.next()) {
						totalRows += rset.getInt(1);
					}
					rset.close();
					stmt.close();
				}
				// 计算同步需要花费的时间
				syncCompleteTime = totalRows / this.plugNumber;

				this.logger.logClass.info(
						"FullSync 评估已经完成, FullSync 将要同步 [" + totalRows + "] 条记录, 预计 [" + syncCompleteTime + "] 秒内完成");
			} catch (SQLException sqlEx) {
				this.logger.logClass.error(
						"Cannot complete fullSync estimation on the source db,error [" + sqlEx.getMessage() + "]");
			}
		}
	}

	public void copyData() throws Exception {
		Statement stmt = this.sourceDB.getMSSQLConnection().createStatement();
		// 对 MSSQL 数据库同步时间进行评估
		mssqlFullSyncEstimate();
		for (syncObjects currentObject : this.objectsToSync) {
			int databaseId = -1;
			String lsn = "";
			String startLsn = "-1";
			String dbName = currentObject.dbName;
			this.sourceDB.getMSSQLConnection().setCatalog(dbName);
			// 查询最大LSN号
			String lsnQuery = "select db_id('" + dbName
					+ "'), max([Current Lsn]) from fn_dblog(null,null) where Operation = 'LOP_COMMIT_XACT'";
			ResultSet lsnRset = stmt.executeQuery(lsnQuery);
			if (lsnRset.next()) {
				databaseId = lsnRset.getInt(1);
				lsn = lsnRset.getString(2);
				startLsn = MSSQLDataStore.convertLsnToDecString(lsn);
			}
			lsnRset.close();
			String isScheamsExistQuery="select count(*) mun from sys.sysschemas where schemaname='"+dbName.toUpperCase()+"'";
			//创建snappydata 的Schemas
			String createScheamsQuery = "CREATE SCHEMA " +dbName.toUpperCase() + ";";
			Statement createScheamsStmt = this.snappyDB.getSnappyConnection().createStatement();
			try {
				ResultSet isScheamsExistRset = createScheamsStmt.executeQuery(isScheamsExistQuery);
				if (isScheamsExistRset.next()) {
					int isScheamsExist = isScheamsExistRset.getInt(1);
					if(isScheamsExist==0){
						createScheamsStmt.executeUpdate(createScheamsQuery);
					}
				}
				
				
			} catch (SQLException sqlEx) {
				createScheamsStmt.close();
				this.logger.logClass
						.error("全量加载数据 - 在目标数据库上 " + sqlEx.getMessage() + " 创建snappydata Scheams 失败" + dbName + " ;");
				throw sqlEx;
			}
			System.out.println("完成创建snappydata Scheams:" + dbName );
			createScheamsStmt.close();
			
			// 得到 Schema, Object 名称
			for (String objectFullName : currentObject.objectsList) {
				this.tableRecordsCount = 0;
				this.tableSyncStartTime = System.currentTimeMillis();
				this.batchT1 = System.currentTimeMillis();
				String schemaName = objectFullName.substring(0, objectFullName.indexOf("."));
				String objectName = objectFullName.substring(objectFullName.indexOf(".") + 1, objectFullName.length());
				try {

					String prepQuery = "SELECT distinct scols.colid, scols.NAME as columnName, types.name AS colType , "
							+ "ISNULL(scols.length, cols.max_length) AS colLen, "
							+ "ISNULL(scols.xprec, cols.precision) AS precision, "
							+ "ISNULL(scols.xscale, cols.scale) AS scale, " + " scols.isnullable " + " FROM  " + dbName
							+ ".sys.tables t " + "INNER JOIN " + dbName + ".sys.schemas s ON s.schema_id=t.schema_id "
							+ "INNER JOIN " + dbName + ".sys.partitions partitions ON t.object_id=partitions.object_id "
							+ "INNER JOIN " + dbName
							+ ".sys.system_internals_partition_columns cols ON cols.partition_id = partitions.partition_id "
							+ "LEFT OUTER JOIN " + dbName
							+ ".sys.syscolumns scols ON scols.id = t.object_id AND scols.colid = cols.partition_column_id "
							+ "INNER JOIN  " + dbName + ".sys.systypes types ON scols.xusertype = types.xusertype "
							+ "WHERE is_ms_shipped = 0 " + "AND cols.is_dropped=0 " + "AND t.type = 'U' "
							+ "AND schema_name(t.schema_id) = ? " + "AND t.name = ? " + "ORDER BY scols.colid";
					// 通过schemaName, tableName 来查询表中的字段信息
					PreparedStatement objectColumns = this.sourceDB.getMSSQLConnection().prepareStatement(prepQuery);
					objectColumns.setString(1, schemaName); // 模式名称
					objectColumns.setString(2, objectName); // 表名称

					ArrayList objectColumnsList = new ArrayList();

					String objectColumnsNames = "";
					ResultSet rsetObjectColumns = objectColumns.executeQuery();
					String columnJavaGetType = ""; // 获得column对应的Java类型
					String columnName = "";
					String columnType = "";
					String columnLength = "";
					byte precision = 0;
					byte scale = 0;
					int columnNulls = 1;
					String createTableQuery = "";
					String dropTableQuery = "";
					while (rsetObjectColumns.next()) {
						columnName = rsetObjectColumns.getString(2);
						columnType = rsetObjectColumns.getString(3).trim();
						columnLength = rsetObjectColumns.getString(4);
						precision = rsetObjectColumns.getByte(5);
						scale = rsetObjectColumns.getByte(6);
						columnNulls = rsetObjectColumns.getInt(7);
						columnJavaGetType = this.snappyDB.getMSSQLPostgresType(sourceDB.getDsType(), columnType,
								columnLength);
						columnDetails columnProp = new columnDetails(columnName, columnType, columnJavaGetType);
						objectColumnsList.add(columnProp);
						if ("".equals(objectColumnsNames)) {
							objectColumnsNames = columnName;
						} else {
							objectColumnsNames = objectColumnsNames + "," + columnName;
						}

						if (columnNulls == 0) {
							createTableQuery = createTableQuery + columnName + " " + columnJavaGetType + " NOT NULL, ";
						} else {
							createTableQuery = createTableQuery + columnName + " " + columnJavaGetType + ", ";
						}
					}
					rsetObjectColumns.close();
					objectColumns.close();
					
					//处理user保留字段问题
					if(createTableQuery.toUpperCase().contains("USER")){
						createTableQuery= createTableQuery.toUpperCase().replaceAll("USER", "USERS");
					}
					
					dropTableQuery = "DROP TABLE IF EXISTS " + dbName+"."+ objectName + ";";
					createTableQuery = "CREATE TABLE " + dbName+"."+objectName + " ("
							+ createTableQuery.substring(0, createTableQuery.length() - 2) + ");";

					Statement createTableStmt = this.snappyDB.getSnappyConnection().createStatement();
					try {
						createTableStmt.executeUpdate(dropTableQuery);
						createTableStmt.executeUpdate(createTableQuery);
					} catch (SQLException sqlEx) {
						createTableStmt.close();
						this.logger.logClass
								.error("全量加载数据 - 在目标数据库上 " + sqlEx.getMessage() + " 建表失败 " + objectName + " ;");
						throw sqlEx;
					}
					System.out.println("完成表创建:" + dbName + "." + schemaName + "." + objectName);
					createTableStmt.close();

					String pkColumnName = objectColumnsNames.split(",")[0];
					ArrayList rowList = new ArrayList();
					// 查询表中每个字段的值
					final String threadPrepQuery = "SELECT " + objectColumnsNames
							+ " FROM (SELECT ROW_NUMBER() OVER(ORDER BY " + pkColumnName + " ASC) AS rownum,"
							+ " * FROM " + dbName + "." + schemaName + ".\"" + objectName
							+ "\") AS tmp WHERE rownum >= ? AND rownum <= ?;";
					// 拆分数据库表同步工作
					ArrayList unitsOfWorkList = splitWork(dbName, schemaName, objectName);

					int threadId = 1;
					final ArrayList threadObjectColumnsList = objectColumnsList;
					final String threadDatabaseName = dbName;
					final String threadSchemaName = schemaName;
					final String threadObjectName = objectName;
					final CountDownLatch doneSignal = new CountDownLatch(unitsOfWorkList.size());
					ArrayList threadsList = new ArrayList();

					int totalCount = 1;
					// 待同步的表为没有数据(空行)
					for (java.util.Iterator iter = unitsOfWorkList.iterator(); iter.hasNext();) {
						final mssqlRowsRange workUnit = (mssqlRowsRange) iter.next();
						if (workUnit.endRowNumber == 0)
							totalCount = workUnit.endRowNumber;

					}

					if (totalCount == 0) {
						continue;
					}
					for (java.util.Iterator iter = unitsOfWorkList.iterator(); iter.hasNext();) {
						final mssqlRowsRange workUnit = (mssqlRowsRange) iter.next();
						final String threadName = "MSSQLcopyDataWorkUnit-" + threadId;

						Thread copyDataWorkUnitThread = new Thread(threadName) {
							public void run() {
								SnappyDataStore threadSnDs = null;
								try {
									String threadUrl = new StringBuilder().append("jdbc:snappydata:thrift://").append(SyncMSSQLSnappy.this.targetHost)
											.append("[").append(SyncMSSQLSnappy.this.targetPort).append("]").toString();
									SyncMSSQLSnappy.this.logger.logClass.debug("连接到Snappy目标数据库");
									SyncMSSQLSnappy.this.logger.logClass.debug("连接 URL=" + threadUrl);
									threadSnDs = new SnappyDataStore("SNAPPY_TARGET_THREAD_" + threadName,
											threadUrl);
									System.out.println("成功连接目标数据库");
									threadSnDs.getSnappyConnection().setAutoCommit(true);
									long time1 = System.currentTimeMillis();
									SyncMSSQLSnappy.this.copyDataWorkUnit(threadPrepQuery, threadObjectColumnsList,
											threadDatabaseName, threadSchemaName, threadObjectName, threadName,
											doneSignal, workUnit, threadSnDs);
									long time2 = System.currentTimeMillis();
									System.out.println("线程同步总计耗时" + (time2 - time1) / 1000 + "秒");
									threadSnDs.closeSnappyConnection();
								} catch (Exception ex) {
									if (threadSnDs != null) {
										threadSnDs.closeSnappyConnection();
									}
								}
							}
						};
						threadsList.add(copyDataWorkUnitThread);
						copyDataWorkUnitThread.start();
						Thread.sleep(100L);
						threadId++;
					}

					doneSignal.await();
					if ("Failed".equals(getFullSyncStatus())) {
						this.logger.logClass
								.error("Failed to finish sync table " + dbName + "." + schemaName + "." + objectName);
						throw new Exception("Full sync error");
					}

					this.tableSyncEndTime = System.currentTimeMillis();
					this.logger.logClass.info("Finished sync table " + dbName + "." + schemaName + "." + objectName
							+ " [" + this.tableRecordsCount + " records] in "
							+ (int) (this.tableSyncEndTime - this.tableSyncStartTime) + " [ms]");

				} catch (Exception ex) {
					this.logger.logClass.error("Error on Full Sync of table " + dbName + "." + objectName);
					this.logger.logClass.debug(ex.getMessage());
					throw ex;
				}
			}

		}
		if (stmt != null)
			stmt.close();
		if (this.sourceDB != null) {
			this.logger.logClass.debug("copyData - end of flow, closing sqlserver connection");
			this.sourceDB.closeMSSConnection();
		}
	}

	public ArrayList<mssqlRowsRange> splitWork(String dbName, String schemaName, String objectName)
			throws SQLException {
		ArrayList mssqlRowsRangeList = new ArrayList();
		Statement stmt = this.sourceDB.getMSSQLConnection().createStatement();
		PreparedStatement prepStmt = null;
		ResultSet rset = null;
		try {
			String query = "select count(*) from " + dbName + "." + schemaName + ".\"" + objectName + "\"";
			rset = stmt.executeQuery(query);
			rset.next();
			int totalRows = rset.getInt(1);
			rset.close();
			int basicWorkUnit = totalRows / this.parallelism;
			int runningWorkUnit;
			if (this.parallelism > 1) {
				runningWorkUnit = 0;
				int workUnitStart = 1;
				for (int i = 1; i <= this.parallelism; i++) {
					runningWorkUnit = basicWorkUnit * i;
					if (i < this.parallelism) {
						mssqlRowsRangeList.add(new mssqlRowsRange(workUnitStart, runningWorkUnit));
						workUnitStart = runningWorkUnit + 1;
					} else {
						mssqlRowsRangeList.add(new mssqlRowsRange(workUnitStart, totalRows));
					}
				}
			} else {
				mssqlRowsRangeList.add(new mssqlRowsRange(1, totalRows));
			}
			return mssqlRowsRangeList;
		} catch (SQLException sqlEx) {
			this.logger.logClass.error("拆分同步任务 - 生成工作单元失败. 错误为: " + sqlEx.getMessage());
			throw sqlEx;
		} finally {
			if (rset != null)
				rset.close();
			if (stmt != null)
				stmt.close();
			if (prepStmt != null)
				prepStmt.close();
		}
	}

	public void run() {
		boolean connected = false;
		try {
			this.logger.logClass.info("数据同步开始");
			// updateFullSyncStatus("Started", this.configurationName);
			System.out.println("开始同步操作");

			try {
				connectToSource();
				connectToTarget();
				connected = true;
			} catch (SQLException sqlEx) {
				this.logger.logClass.error("连接源数据库已经失败, 发生错误 [" + sqlEx.getMessage() + "]");
			}

			if (connected) {

				copyData();
			}

			this.logger.logClass.info("数据全量同步已经成功完成!");
			// updateFullSyncStatus("Completed", this.configurationName);
		} catch (InterruptedException inEx) {
			interrupt();
			this.logger.logClass.info("数据全量同步已经停止!");
			// updateFullSyncStatus("Stopped", this.configurationName);
		} catch (Exception gex) {
			this.logger.logClass.error(gex.getMessage());
			this.logger.logClass.error("完成数据全量同步失败!");
			// updateFullSyncStatus("Failed", this.configurationName);
		} finally {
			this.sourceDB.closeMSSConnection();
		}
	}

	public void copyDataWorkUnit(String sqlToRun, ArrayList<columnDetails> objectColumnsList, String databaseName,
			String schemaName, String objectName, String threadName, CountDownLatch doneSignal,
			mssqlRowsRange currentWorkUnit, SnappyDataStore threadSnDs)
			throws SQLException, InterruptedException, ParseException, Exception {
		int count = 0;
		long gpt1 = 0L;
		long gpt2 = 0L;
		long mssqlt1 = 0L;
		long mssqlt2 = 0L;
		int mssqlTime = 0;
		long batchT2 = 0L;

		int columnsTime = 0;
		int columnIndex = 0;
		this.logger.logClass.debug("启动同步线程 : " + threadName + ". ");
		Connection threadMSSQLConnection = this.sourceDB.getMSSQLConnection();
		PreparedStatement objectData = threadMSSQLConnection.prepareStatement(sqlToRun);
		String columnStringValue = "";
		byte[] columnByteValue = null;
		boolean columnBooleanValue = true;
		short columnShortValue = 0;
		int columnIntValue = 0;
		long columnLongValue = 0L;
		float columnFloatValue = 0F;
		double columnDoubleValue = 0D;
		BigDecimal columnDecimalValue;
		int batchCounter = 0;
		int gpDeltaTime = 0;
		int totalDeltaTime = 0;
		int logcount = 0;
		// 执行具体的插入值
		String insertSql = "INSERT INTO " + databaseName+"."+ objectName + " VALUES(";
		for (int i = 0; i < objectColumnsList.size(); i++) {
			insertSql = insertSql + "?,";
		}
		insertSql = insertSql.substring(0, insertSql.length() - 1) + ")";
		/*if(insertSql.toUpperCase().contains("USER")){
			insertSql=insertSql.replaceAll("USER", "USERS");
		}*/
		
		PreparedStatement insertStmt = threadSnDs.getSnappyConnection().prepareStatement(insertSql);
		ResultSet rsetObjectData = null;
		try {
			objectData.setInt(1, currentWorkUnit.startRowNumber);
			objectData.setInt(2, currentWorkUnit.endRowNumber);
			System.out.println("开始行" + currentWorkUnit.startRowNumber + ", 结束行" + currentWorkUnit.endRowNumber);
			rsetObjectData = objectData.executeQuery();
			Object syncCounterObject = new Object();
			mssqlt1 = System.currentTimeMillis();
			long batchT1 = System.currentTimeMillis();
			while (rsetObjectData.next()) {
				mssqlt2 = System.currentTimeMillis();
				mssqlTime += (int) (mssqlt2 - mssqlt1);
				// 获得时间差
				columnIndex = 0;

				long columnst1 = System.currentTimeMillis();
				for (columnDetails currentColumn : objectColumnsList) {
					//处理user保留字段问题
					if(currentColumn.columnName.toUpperCase().equals("USER")){
						currentColumn.columnName="USERS";
					}
					columnIndex++;
					switch (currentColumn.columnType.toUpperCase()) {
					case "BINARY":
					case "VARBINARY":
					case "IMAGE":
					case "BIT":
						columnByteValue = rsetObjectData.getBytes(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnByteValue, insertStmt,
								null);
						break;
					case "CHAR":
					case "NCHAR":
					case "VARCHAR":
					case "NVARCHAR":
					case "SYSNAME":
					case "SMALLDATETIME":
						columnStringValue = rsetObjectData.getString(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue, insertStmt,
								null);

						break;
					case "TEXT":
					case "NTEXT":
					case "UNIQUEIDENTIFIER":
					case "XML":
						columnStringValue = rsetObjectData.getString(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue, insertStmt,
								null);

						break;

					case "TINYINT":
					case "SMALLINT":
					case "INT":
					case "INTEGER":
						columnIntValue = rsetObjectData.getInt(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnIntValue, insertStmt,
								null);

						break;

					case "BIGINT":
						columnLongValue = rsetObjectData.getLong(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnLongValue, insertStmt,
								null);

						break;

					case "REAL":
					case "FLOAT":
						columnFloatValue = rsetObjectData.getFloat(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnFloatValue, insertStmt,
								null);

						break;

					case "SMALLMONEY":
					case "MONEY":
					case "DECIMAL":
					case "NUMERIC":
						columnDecimalValue = rsetObjectData.getBigDecimal(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnDecimalValue, insertStmt,
								null);

						break;

					case "DOUBLE":
						columnDoubleValue = rsetObjectData.getDouble(columnIndex);
						threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnDoubleValue, insertStmt,
								null);

						break;

					case "DATE":
						if ((columnStringValue == null || columnStringValue.isEmpty())
								|| (columnStringValue.equals("null")) || (columnStringValue.equals("NULL")))
							threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue,
									insertStmt, new SimpleDateFormat(this.dateFormat));
						else {
							columnStringValue = this.outputDateFormat.format(rsetObjectData.getDate(columnIndex));
							threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue,
									insertStmt, new SimpleDateFormat(this.dateFormat));
						}
						break;
					case "TIME":
						if ((columnStringValue == null || columnStringValue.isEmpty())
								|| (columnStringValue.equals("null")) || (columnStringValue.equals("NULL")))
							threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue,
									insertStmt, new SimpleDateFormat(this.dateFormat));
						else {
							columnStringValue = this.outputDateFormat.format(rsetObjectData.getTime(columnIndex));
							threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue,
									insertStmt, new SimpleDateFormat(this.dateFormat));
						}
						break;

					case "DATETIME":
					case "TIME WITH TIME ZONE":
					case "TIMESTAMP WITH TIME ZONE":
					case "TIMESTAMP":
						if ((columnStringValue == null || columnStringValue.isEmpty())
								|| (columnStringValue.equals("null")) || (columnStringValue.equals("NULL")))
							threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue,
									insertStmt, new SimpleDateFormat(this.dateFormat));
						else {
							columnStringValue = this.outputDateFormat.format(rsetObjectData.getTimestamp(columnIndex));
							threadSnDs.addMSSQLFieldValue(currentColumn.columnType, columnIndex, columnStringValue,
									insertStmt, new SimpleDateFormat(this.dateFormat));

						}
					}

				}
				long columnst2 = System.currentTimeMillis();
				columnsTime += (int) (columnst2 - columnst1);

				insertStmt.addBatch();
				count++;
				if (count % this.batchSize == 0) {
					count = 0;
					gpt1 = System.currentTimeMillis();
					try {
						insertStmt.executeBatch();
					} catch (SQLException sqlEx) {
						this.logger.logClass.error("插入到源端数据库的  " + objectName + " 表失败 : " + sqlEx.getMessage());
						throw sqlEx;
					}
					gpt2 = System.currentTimeMillis();
					gpDeltaTime += (int) (gpt2 - gpt1);
					batchCounter++;
					batchT2 = System.currentTimeMillis();
					totalDeltaTime += (int) (batchT2 - batchT1);
					batchT1 = System.currentTimeMillis();
				}

				logcount++;
				if (logcount % this.fullSyncloggingBatchSize == 0) {
					this.logger.logClass.debug("完全同步表进度 " + databaseName + "." + schemaName + "." + objectName + ": "
							+ this.fullSyncloggingBatchSize + " 条记录, 时间 " + totalDeltaTime + "毫秒|"
							+ " SnappyData  写入时间 (ms):" + gpDeltaTime + "|" + " MSSQL 读取时间 (ms):" + mssqlTime + "|"
							+ "  列设置时间 (ms):" + columnsTime);

					totalDeltaTime = 0;
					gpDeltaTime = 0;
					mssqlTime = 0;
					columnsTime = 0;
					logcount = 0;
				}
				mssqlt1 = System.currentTimeMillis();
				// 插入到目标库
				gpt1 = System.currentTimeMillis();
				try {
					insertStmt.executeBatch();
				} catch (SQLException sqlEx) {
					this.logger.logClass.error("在源端数据库表  " + objectName + " 插入发生错误: " + sqlEx.getMessage());
					throw sqlEx;
				}
				gpt2 = System.currentTimeMillis();
			}

			insertStmt.close();
			rsetObjectData.close();
			objectData.close();

			synchronized (syncCounterObject) {
				this.tableRecordsCount += count + this.batchSize * batchCounter;
			}
			batchT2 = System.currentTimeMillis();
			this.logger.logClass.debug("完全同步表进度 " + databaseName + "." + schemaName + "." + objectName + ": " + count
					+ " records in " + (int) (batchT2 - batchT1) + "ms|" + " Snappydata write (ms):"
					+ (int) (gpt2 - gpt1) + "|" + " MSSQL read time (ms):" + mssqlTime);
			System.out.println("完全同步表进度 " + databaseName + "." + schemaName + "." + objectName + ": " + count
					+ " records in " + (int) (batchT2 - batchT1) + "ms|" + " Snappydata write (ms):"
					+ (int) (gpt2 - gpt1) + "|" + " MSSQL read time (ms):" + mssqlTime);
		} catch (Exception ex) {
			this.logger.logClass.error("copyDataWorkUnit - failed to complete. Error: " + ex.getMessage());
			System.out.println(ex.getMessage());
			throw ex;
		}

		finally {
			if (rsetObjectData != null)
				rsetObjectData.close();
			if (objectData != null)
				objectData.close();
			this.logger.logClass.debug("Thread [ " + threadName + "] exiting, closing sql connection to source DB");

			if ((insertStmt != null) && (!insertStmt.isClosed())) {
				insertStmt.close();
			}
			doneSignal.countDown();
		}
	}

	static class columnDetails {
		String columnName = "";
		String columnType = "";
		String columnJavaGetType = "";

		public columnDetails(String inColumnName, String inColumnType) {
			this.columnName = inColumnName;
			this.columnType = inColumnType;
		}

		public columnDetails(String inColumnName, String inColumnType, String inColumnJavaGetType) {
			this.columnName = inColumnName;
			this.columnType = inColumnType;
			this.columnJavaGetType = inColumnJavaGetType;
		}
	}

	static class mssqlRowsRange {
		int startRowNumber = 0;
		int endRowNumber = 0;

		public mssqlRowsRange(int inStart, int inEnd) {
			this.startRowNumber = inStart;
			this.endRowNumber = inEnd;
		}
	}

	private class syncObjects {
		String dbName = "";
		ArrayList<String> objectsList = new ArrayList();

		public syncObjects(String inDbName) {
			this.dbName = inDbName;
		}

		public void addObject(String inObjectName) {
			this.objectsList.add(inObjectName);
		}

		public int getSize() {
			return this.objectsList.size();
		}
	}

	// 获得源数据库的表清单
	public void getObjectsList() throws Exception {
		for (int i = 0; i < this.databasesAndObjects.getSize(); i++) {
			String tablesQuery = "SELECT SCHEMA_NAME(Schema_id), t.name FROM sys.tables  t LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id WHERE (ep.class_desc IS NULL OR (ep.class_desc <> 'OBJECT_OR_COLUMN' AND ep.name <> 'microsoft_database_tools_support')) AND  t.object_id in "
					+ MSSQLRepObjects.getGenericInclude();

			Statement tablesStmt = this.sourceDB.getMSSQLConnection().createStatement();
			String tableName = "";
			String dbName = "";
			// 从DB列表中取得每个dbName
			ResultSet tablesRset = null;
			dbName = this.databasesAndObjects.getDbNameByIndex(i);
			String whereClause = "";
			String excludeObjects = this.databasesAndObjects.getExcludeObjects(dbName);
			if (!excludeObjects.isEmpty()) {
				whereClause = whereClause + " and object_id not in " + excludeObjects;
			}

			// 创建需要同步的数据库列表
			syncObjects currentDbObjects = new syncObjects(dbName);
			try {
				// 设置 Catalog 数据库
				this.sourceDB.getMSSQLConnection().setCatalog(dbName);
				// 查询 SchemaName.TableName, 并将当前表添加到同步列表中
				tablesRset = tablesStmt.executeQuery(tablesQuery);
				while (tablesRset.next()) {
					String currentTable = tablesRset.getString(1) + "." + tablesRset.getString(2);

					currentDbObjects.addObject(currentTable);
				}
				if ((tablesStmt != null) && (!tablesStmt.isClosed()))
					tablesStmt.close();
				if ((tablesRset != null) && (!tablesRset.isClosed()))
					tablesRset.close();
			} catch (SQLException sqlEx) {
				if ((tablesStmt != null) && (!tablesStmt.isClosed()))
					tablesStmt.close();
				if ((tablesRset != null) && (!tablesRset.isClosed()))
					tablesRset.close();
				throw new SQLException("Unable to get tables list from source DB .");
			}
			// 将同步的数据库列表添加到大列表中
			this.objectsToSync.add(currentDbObjects);
			if ((tablesStmt != null) && (!tablesStmt.isClosed()))
				tablesStmt.close();
			if ((tablesRset != null) && (!tablesRset.isClosed()))
				tablesRset.close();
		}
	}

	public static void main(String[] args) {
		try {
			Thread syncThread = new Thread(new SyncMSSQLSnappy("testmssql2snappy"));
			syncThread.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
