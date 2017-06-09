package com.bloom.dbsync.db;

import com.bloom.dbsync.fileLogger.LogWriter;
import com.google.api.client.util.Types;
import com.pivotal.gemfirexd.internal.client.am.Clob;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Scanner;

import javax.sql.DataSource;

import org.apache.derby.iapi.types.XML;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.ds.PGSimpleDataSource;

public class SnappyDataStore extends DataStore {
	private DataSource snappyDataSource;
	private Connection snappyConnection;
	private Properties connInfo = new Properties();

	public SnappyDataStore(String dsLogicalName, String dsConnectionUrl) throws SQLException {
		super(dsLogicalName, dsConnectionUrl, "SparkDB");
		this.logger = new LogWriter(SnappyDataStore.class);

		this.snappyConnection = DriverManager.getConnection(dsConnectionUrl);
		this.logger.logClass.info("连接到 " + dsConnectionUrl);

		Statement stm = this.snappyConnection.createStatement();
		stm.close();
		stm = null;
	}

	public Connection getSnappyConnection() {
		return this.snappyConnection;
	}

	public Properties getConnectionInfo() {
		return this.connInfo;
	}

	public void printDataStoreInfo() {
		System.out.println("************** Data Store Info *******************************");
		System.out.println("Data Store type             : " + getDsType());
		System.out.println("Data Store logcial name     : " + getDsLogicalName());
		System.out.println("Data Store connection URL   : " + getDsConnectionUrl());
		System.out.println("************** Data Store Info *******************************");
	}

	public static String createStringIncludeSchemaAndTable(String schemasDefinition, boolean addNullOwner,
			String ownerColmnName) {
		String answer = "";
		boolean firstTime = true;
		if (!"ALL".equals(schemasDefinition)) {
			answer = answer + " and (";
			Scanner s = new Scanner(schemasDefinition);
			s.useDelimiter(";");
			while (s.hasNext()) {
				boolean haveTable = false;
				if (!firstTime)
					answer = answer + " or ";
				String currentString = s.next();
				int finishSchemaName = currentString.indexOf(":");
				String schemaName;
				if (finishSchemaName == -1) {
					schemaName = currentString;
				} else {
					schemaName = currentString.substring(0, finishSchemaName);
					haveTable = true;
				}

				answer = answer + "(" + ownerColmnName + " = '" + schemaName + "'";
				if (haveTable) {
					String tables = currentString.replace(schemaName + ":", "");
					answer = answer + " and TABLE_NAME in (";
					Scanner s2 = new Scanner(tables);
					s2.useDelimiter(",");
					while (s2.hasNext()) {
						answer = answer + "'" + s2.next() + "',";
					}
					answer = answer.substring(0, answer.length() - 1);
					s2.close();
					answer = answer + ")";
				}
				answer = answer + ")";

				firstTime = false;
			}

			if (addNullOwner) {
				answer = answer + " OR (" + ownerColmnName + " is NULL and operation='COMMIT') " + " OR ("
						+ ownerColmnName + " is NULL and operation='DDL' and INFO like 'USER DDL%')";
			}

			s.close();
			answer = answer + ")";
		}
		return answer.toUpperCase();
	}

	public static String createStringExcludeSchemaAndTable(String schemasDefinition, String ownerColmnName) {
		String answer = "";
		if (!"NONE".equals(schemasDefinition)) {
			String withTable = "";
			String withoutTable = "";
			Scanner s = new Scanner(schemasDefinition);
			s.useDelimiter(";");
			while (s.hasNext()) {
				String currentString = s.next();
				int finishSchemaName = currentString.indexOf(":");

				if (finishSchemaName == -1) {
					String schemaName = currentString;
					withoutTable = withoutTable + "'" + schemaName + "',";
				} else {
					String schemaName = currentString.substring(0, finishSchemaName);
					String tables = currentString.replaceAll(schemaName + ":", "");
					Scanner s2 = new Scanner(tables);
					s2.useDelimiter(",");
					while (s2.hasNext()) {
						String table = s2.next();
						withTable = withTable + "'" + schemaName + "." + table + "',";
					}
					s2.close();
				}
			}
			s.close();
			if (!"".equals(withTable)) {
				withTable = withTable.substring(0, withTable.length() - 1);
				answer = answer + " and (" + ownerColmnName + "||'.'||TABLE_NAME not in(" + withTable + "))";
			}
			if (!"".equals(withoutTable)) {
				withoutTable = withoutTable.substring(0, withoutTable.length() - 1);
				answer = answer + " and (nvl(" + ownerColmnName + ",'NoOwner') not in(" + withoutTable + "))";
			}
		}
		return answer.toUpperCase();
	}

	public static String createStringIncludeCommands(String includeCommands) {
		String answer = "";
		boolean firstTime = true;
		if (!"ALL".equals(includeCommands)) {
			answer = answer + " and OPERATION IN (";
			Scanner s = new Scanner(includeCommands);
			s.useDelimiter(";");
			while (s.hasNext()) {
				String currentCommand = s.next();
				if (firstTime) {
					answer = answer + "'" + currentCommand + "'";
				} else {
					answer = answer + ",'" + currentCommand + "'";
				}
				firstTime = false;
			}
			s.close();
			answer = answer + ") ";
		}
		return answer.toUpperCase();
	}

	public static String createStringExcludeCommands(String excludeCommands) {
		String answer = "";
		boolean firstTime = true;
		if (!"NONE".equals(excludeCommands)) {
			answer = answer + " and OPERATION NOT IN (";
			Scanner s = new Scanner(excludeCommands);
			s.useDelimiter(";");
			while (s.hasNext()) {
				String currentCommand = s.next();
				if (firstTime) {
					answer = answer + "'" + currentCommand + "'";
				} else {
					answer = answer + ",'" + currentCommand + "'";
				}
				firstTime = false;
			}
			s.close();
			answer = answer + ") ";
		}
		return answer.toUpperCase();
	}

	public void closeSnappyConnection() {
		try {
			if (this.snappyConnection != null)
				this.snappyConnection.close();
		} catch (SQLException sqlEx) {
			this.logger.logClass.error(
					"Failed to close SparkDB connection " + this.dsConnectionUrl + ". Error: " + sqlEx.getMessage());
		}

	}

	public String getSnappyType(String dbType, String sourceType, String sourceLength) {
		String snappyType = "";
		if (sourceLength == "-1" || sourceLength.equals("-1"))
			sourceLength = "10485760";
		switch (sourceType.toUpperCase()) {

		case "BINARY":
		case "VARBINARY":
		case "IMAGE":

			snappyType = "BYTEA";
			break;
		case "BIT":
			// gpType = "INT";
			// break;
		case "BOOLEAN":
			snappyType = "BOOLEAN";
			break;
		case "CHAR":
			snappyType = "CHAR(" + sourceLength + ")";
			break;
		case "NCHAR":
		case "VARCHAR":
		case "NVARCHAR":
		case "SYSNAME":
			snappyType = "VARCHAR(" + sourceLength + ")";
			break;

		case "TEXT":
		case "NTEXT":
		case "UNIQUEIDENTIFIER":
			snappyType = "TEXT";
			break;
		case "TINYINT":
			snappyType = "SMALLINT";
			break;
		case "SMALLINT":
			snappyType = "SMALLINT";
			break;
		case "INT":
			snappyType = "INT";
			break;
		case "INTEGER":
			snappyType = "INTEGER";
			break;
		case "BIGINT":
			snappyType = "BIGINT";
			break;
		case "DOUBLE":
			snappyType = "DOUBLE PRECISION";
			break;

		case "FLOAT":
		case "REAL":
		case "SMALLMONEY":
		case "MONEY":
		case "NUMERIC":
			snappyType = "NUMERIC";
			break;
		case "DECIMAL":
			snappyType = "DECIMAL";
			break;
		case "SMALLDATETIME":
			snappyType = "VARCHAR(100)";
			break;
		case "DATETIME":
			snappyType = "DATE";
			break;
		case "DATE":
			snappyType = "DATE";
			break;
		case "TIME":
			snappyType = "TIME";
			break;
		case "TIMESTAMP":
			if (dbType == "MSSQL")
				snappyType = "BYTEA";
			else
				snappyType = "TIMESTAMP";
			break;
		}

		return snappyType;
	}

	public void addFieldValue(String fieldType, int fieldPosition, Object fieldValue,
			PreparedStatement commandStatement, SimpleDateFormat inputDateFormat) throws Exception {
		switch (fieldType.toUpperCase()) {
		case "BINARY":
		case "VARBINARY":
		case "IMAGE":

			byte[] byteValue = (byte[]) fieldValue;
			if (byteValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.BINARY);
			} else {
				commandStatement.setBytes(fieldPosition, byteValue);
			}
			break;
		case "BIT":
		case "BOOLEAN":
			boolean boolValue = (boolean) fieldValue;
			commandStatement.setBoolean(fieldPosition, boolValue);
			break;
		case "CHAR":
		case "NCHAR":
		case "VARCHAR":
		case "NVARCHAR":
		case "SYSNAME":
		case "SMALLDATETIME":
			String strValue = (String) fieldValue;
			if ((strValue == null || strValue.isEmpty()) || (strValue.equals("null")) || (strValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.VARCHAR);
			} else {
				commandStatement.setString(fieldPosition, strValue);
			}
			break;
		case "TEXT":
		case "NTEXT":
			String txtValue = (String) fieldValue;
			if ((txtValue == null || txtValue.isEmpty()) || (txtValue.equals("null")) || (txtValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.CLOB);
			} else {
				commandStatement.setString(fieldPosition, txtValue);
			}
			break;

		case "TINYINT":
		case "SMALLINT":
			Short sintValue = (Short) fieldValue;
			if ((sintValue == null) || (sintValue.equals("null")) || (sintValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TINYINT);
			} else {
				commandStatement.setShort(fieldPosition, sintValue);
			}
			break;

		case "INT":
		case "INTEGER":
			Integer intValue = (Integer) fieldValue;
			if ((intValue == null) || (intValue.equals("null")) || (intValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.INTEGER);
			} else {
				commandStatement.setInt(fieldPosition, intValue);
			}
			break;

		case "BIGINT":
			Long longValue = (Long) fieldValue;
			if ((longValue == null) || (longValue.equals("null")) || (longValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.BIGINT);
			} else {
				commandStatement.setLong(fieldPosition, longValue);
			}
			break;

		case "REAL":
		case "FLOAT":
			Float floatValue = (Float) fieldValue;
			if ((floatValue == null) || (floatValue.equals("null")) || (floatValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.FLOAT);
			} else {
				commandStatement.setFloat(fieldPosition, floatValue);
			}
			break;

		case "MONEY":
			BigDecimal moneyValue = (BigDecimal) fieldValue;

			if ((moneyValue == null) || (moneyValue.equals("null")) || (moneyValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
			} else {
				commandStatement.setBigDecimal(fieldPosition, moneyValue);
			}
			break;
		case "SMALLMONEY":
		case "DECIMAL":
			BigDecimal decimalValue = (BigDecimal) fieldValue;
			if ((decimalValue == null) || (decimalValue.equals("null")) || (decimalValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.DECIMAL);
			} else {
				commandStatement.setBigDecimal(fieldPosition, decimalValue);
			}
			break;

		case "NUMERIC":
			BigDecimal numbericValue = (BigDecimal) fieldValue;
			if ((numbericValue == null) || (numbericValue.equals("null")) || (numbericValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
			} else {
				commandStatement.setBigDecimal(fieldPosition, numbericValue);
			}
			break;

		case "DOUBLE":
			Double doubleValue = (Double) fieldValue;
			if ((doubleValue == null) || (doubleValue.equals("null")) || (doubleValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.DOUBLE);
			} else {
				commandStatement.setDouble(fieldPosition, doubleValue);
			}
			break;
		case "DATE":
			String dateValue = (String) fieldValue;
			if ((dateValue == null) || (dateValue.equals("null")) || (dateValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.DATE);
			} else {
				commandStatement.setDate(fieldPosition, new java.sql.Date(inputDateFormat.parse(dateValue).getTime()));
			}
			break;
		case "TIME":
			String timeValue = (String) fieldValue;

			if ((timeValue == null) || (timeValue.equals("null")) || (timeValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TIME);
			} else {
				commandStatement.setTime(fieldPosition, new Time(inputDateFormat.parse(timeValue).getTime()));
			}
			break;
		case "DATETIME":
		case "TIME WITH TIME ZONE":
		case "TIMESTAMP WITH TIME ZONE":
		case "TIMESTAMP":
			String timestampValue = (String) fieldValue;
			if ((timestampValue == null || timestampValue.isEmpty()) || (fieldValue.equals("null"))
					|| (fieldValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TIMESTAMP);
			} else {
				commandStatement.setTimestamp(fieldPosition,
						new Timestamp(inputDateFormat.parse(timestampValue).getTime()));
			}
			break;
		}
	}

	public String getMSSQLPostgresType(String dbType, String sourceType, String sourceLength) {
		String gpType = "";
		if (sourceLength == "-1" || sourceLength.equals("-1"))
			sourceLength = "10485760";
		switch (sourceType.toUpperCase()) {

		case "BINARY":
		case "VARBINARY":
		case "IMAGE":
		case "BIT":
			gpType = "BLOB";
			break;
		case "CHAR":
			gpType = "CHAR(" + sourceLength + ")";
			break;
		case "NCHAR":
		case "VARCHAR":
		case "NVARCHAR":
		case "SYSNAME":
			gpType = "LONG VARCHAR";
			break;
		case "TEXT":
		case "NTEXT":
		case "UNIQUEIDENTIFIER":
		case "XML":
			gpType = "CLOB";
			break;
		case "TINYINT":
		case "SMALLINT":
		case "INT":
		case "INTEGER":
			gpType = "INTEGER";
			break;
		case "BIGINT":
			gpType = "BIGINT";
			break;
		case "DOUBLE":
			gpType = "DOUBLE";
			break;
		case "REAL":
			gpType = "REAL";
			break;
		case "FLOAT":
			gpType = "FLOAT";
			break;

		case "SMALLMONEY":
		case "MONEY":
		case "DECIMAL":
		case "NUMERIC":
			gpType = "NUMERIC";
			break;
		case "SMALLDATETIME":
			gpType = "VARCHAR(100)";
			break;
		case "DATE":
			gpType = "DATE";
			break;
		case "TIME":
			gpType = "TIME";
			break;
		case "DATETIME":
		case "TIME WITH TIME ZONE":
		case "TIMESTAMP WITH TIME ZONE":
		case "TIMESTAMP":
			gpType = "TIMESTAMP";
			break;
		}

		return gpType;
	}

	public void addMSSQLFieldValue(String fieldType, int fieldPosition, Object fieldValue,
			PreparedStatement commandStatement, SimpleDateFormat inputDateFormat) throws Exception {
		switch (fieldType.toUpperCase()) {
		case "BINARY":
		case "VARBINARY":
		case "IMAGE":
		case "BIT":
			byte[] byteValue = (byte[]) fieldValue;
			if (byteValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.BLOB);
			} else {
				commandStatement.setBytes(fieldPosition, byteValue);
			}
			break;
		case "CHAR":
			String strValue = (String) fieldValue;
			if ((strValue == null || strValue.isEmpty()) || (strValue.equals("null")) || (strValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.CHAR);
			} else {
				commandStatement.setString(fieldPosition, strValue);
			}
			break;
		case "NCHAR":
		case "VARCHAR":
		case "NVARCHAR":
		case "SYSNAME":
			String strValues = (String) fieldValue;
			if ((strValues == null || strValues.isEmpty()) || (strValues.equals("null"))
					|| (strValues.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.LONGVARCHAR);
			} else {
				commandStatement.setString(fieldPosition, strValues);
			}
			break;
		case "TEXT":
		case "NTEXT":
		case "UNIQUEIDENTIFIER":
		case "XML":
			String txtValue = (String) fieldValue;
			if ((txtValue == null || txtValue.isEmpty()) || (txtValue.equals("null")) || (txtValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.CLOB);
			} else {
				commandStatement.setString(fieldPosition, txtValue);
			}
			break;

		case "TINYINT":
		case "SMALLINT":
		case "INT":
		case "INTEGER":
			Integer intValue = (Integer) fieldValue;
			if ((intValue == null) || (intValue.equals("null")) || (intValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.INTEGER);
			} else {
				commandStatement.setInt(fieldPosition, intValue);
			}
			break;

		case "BIGINT":
			Long longValue = (Long) fieldValue;
			if ((longValue == null) || (longValue.equals("null")) || (longValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.BIGINT);
			} else {
				commandStatement.setLong(fieldPosition, longValue);
			}
			break;

		case "REAL":
			Float realValue = (Float) fieldValue;
			if ((realValue == null) || (realValue.equals("null")) || (realValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.REAL);
			} else {
				commandStatement.setFloat(fieldPosition, realValue);
			}
			break;
		case "FLOAT":
			Float floatValue = (Float) fieldValue;
			if ((floatValue == null) || (floatValue.equals("null")) || (floatValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.FLOAT);
			} else {
				commandStatement.setFloat(fieldPosition, floatValue);
			}
			break;
		case "DOUBLE":
			Double doubleValue = (Double) fieldValue;
			if ((doubleValue == null) || (doubleValue.equals("null")) || (doubleValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.DOUBLE);
			} else {
				commandStatement.setDouble(fieldPosition, doubleValue);
			}
			break;
		case "SMALLMONEY":
		case "MONEY":
		case "DECIMAL":
		case "NUMERIC":
			BigDecimal numbericValue = (BigDecimal) fieldValue;
			if ((numbericValue == null) || (numbericValue.equals("null")) || (numbericValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
			} else {
				commandStatement.setBigDecimal(fieldPosition, numbericValue);
			}
			break;

		case "SMALLDATETIME":
			String strVarValue = (String) fieldValue;
			if ((strVarValue == null || strVarValue.isEmpty()) || (strVarValue.equals("null"))
					|| (strVarValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.VARCHAR);
			} else {
				commandStatement.setString(fieldPosition, strVarValue);
			}
			break;
		case "DATE":
			String dateValue = (String) fieldValue;
			if ((dateValue == null) || (dateValue.equals("null")) || (dateValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.DATE);
			} else {
				commandStatement.setDate(fieldPosition, new java.sql.Date(inputDateFormat.parse(dateValue).getTime()));
			}
			break;
		case "TIME":
			String timeValue = (String) fieldValue;

			if ((timeValue == null) || (timeValue.equals("null")) || (timeValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TIME);
			} else {
				commandStatement.setTime(fieldPosition, new Time(inputDateFormat.parse(timeValue).getTime()));
			}
			break;
		case "DATETIME":
		case "TIME WITH TIME ZONE":
		case "TIMESTAMP WITH TIME ZONE":
		case "TIMESTAMP":
			String timestampValue = (String) fieldValue;
			if ((timestampValue == null || timestampValue.isEmpty()) || (fieldValue.equals("null"))
					|| (fieldValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TIMESTAMP);
			} else {
				commandStatement.setTimestamp(fieldPosition,
						new Timestamp(inputDateFormat.parse(timestampValue).getTime()));
			}
			break;
		}
	}
}
