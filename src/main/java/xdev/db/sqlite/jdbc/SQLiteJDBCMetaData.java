/*
 * SqlEngine Database Adapter SQLite - XAPI SqlEngine Database Adapter for SQLite
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.sqlite.jdbc;




import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCDataSource;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;
import xdev.util.ProgressMonitor;
import xdev.vt.Cardinality;
import xdev.vt.EntityRelationship;
import xdev.vt.EntityRelationship.Entity;
import xdev.vt.EntityRelationshipModel;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;


public class SQLiteJDBCMetaData extends JDBCMetaData
{
	public SQLiteJDBCMetaData(SQLiteJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	protected String getSchema(JDBCDataSource dataSource)
	{
		return null;
	}
	
	
	@Override
	protected String getCatalog(JDBCDataSource dataSource)
	{
		return null;
	}
	
	
	@Override
	public EntityRelationshipModel getEntityRelationshipModel(ProgressMonitor monitor,
			TableInfo... tableInfos) throws DBException
	{
		monitor.beginTask("",tableInfos.length);
		
		EntityRelationshipModel model = new EntityRelationshipModel();
		
		try
		{
			List<String> tables = new ArrayList<>();
			for(TableInfo table : tableInfos)
			{
				if(table.getType() == TableType.TABLE)
				{
					tables.add(table.getName());
				}
			}
			Collections.sort(tables);
			
			ConnectionProvider connectionProvider = dataSource.getConnectionProvider();
			
			try(Connection connection = connectionProvider.getConnection())
			{
				DatabaseMetaData meta = connection.getMetaData();
				String catalog = getCatalog(dataSource);
				String schema = getSchema(dataSource);
				int done = 0;
				
				for(String table : tables)
				{
					if(monitor.isCanceled())
					{
						break;
					}
					
					monitor.setTaskName(table);
					
					try(ResultSet rs = meta.getExportedKeys(catalog, schema, table))
					{
						String pkTable = null;
						String fkTable = null;
						List<String> pkColumns = new ArrayList<>();
						List<String> fkColumns = new ArrayList<>();
						
						while(rs.next())
						{
							short keySeq = rs.getShort("KEY_SEQ");
							
							if(keySeq == 1
								&& !pkColumns.isEmpty()
								&& tables.contains(pkTable)
								&& tables.contains(fkTable)
							)
							{
								buildEntityRelationship(model, pkTable, fkTable, pkColumns, fkColumns);
							}
							
							pkTable = rs.getString("PKTABLE_NAME").toUpperCase();
							fkTable = rs.getString("FKTABLE_NAME").toUpperCase();
							
							pkColumns.add(rs.getString("PKCOLUMN_NAME").toUpperCase());
							fkColumns.add(rs.getString("FKCOLUMN_NAME").toUpperCase());
							
						}
						
						if(!pkColumns.isEmpty()
							&& tables.contains(pkTable)
							&& tables.contains(fkTable)
						)
						{
							buildEntityRelationship(model, pkTable, fkTable, pkColumns, fkColumns);
						}
					}
					
					monitor.worked(++done);
				}
			}
		}
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return model;
	}
	
	private static void buildEntityRelationship(
		EntityRelationshipModel model,
		String pkTable,
		String fkTable,
		List<String> pkColumns,
		List<String> fkColumns)
	{
		model.add(new EntityRelationship(
			new Entity(pkTable, pkColumns.toArray(new String[pkColumns.size()]), Cardinality.ONE),
			new Entity(fkTable, fkColumns.toArray(new String[fkColumns.size()]), Cardinality.MANY))
		);
		pkColumns.clear();
		fkColumns.clear();
	}
	
	@Override
	protected TableMetaData getTableMetaData(JDBCConnection jdbcConnection, DatabaseMetaData meta,
			int flags, TableInfo table) throws DBException, SQLException
	{
		String catalog = getCatalog(dataSource);
		String schema = getSchema(dataSource);
		
		String tableName = table.getName();
		Table tableIdentity = new Table(tableName,"META_DUMMY");
		
		Map<String, Object> defaultValues = new HashMap<>();
		Map<String, DataType> dataTypeValues = new HashMap<>();
		
		ResultSet rs = meta.getColumns(catalog,schema,tableName,null);
		while(rs.next())
		{
			fillDataTypeValues(defaultValues, dataTypeValues, rs);
		}
		rs.close();
		
		SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		Result result = jdbcConnection.query(select);
		int cc = result.getColumnCount();
		ColumnMetaData[] columns = new ColumnMetaData[cc];
		
		for(int i = 0; i < cc; i++)
		{
			fillColumnMetaData(tableName, defaultValues, dataTypeValues, result, columns, i);
			
		}
		result.close();
		
		Map<IndexInfo, Set<String>> indexMap = new LinkedHashMap<>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			Set<String> primaryKeyColumns = new HashSet<>();
			rs = meta.getPrimaryKeys(catalog,schema,tableName);
			
			while(rs.next())
			{
				final String pkColName = rs.getString("COLUMN_NAME");
				/*
				 * fix for issue XDEVAPI-154
				 * the jdbc method #getPrimaryKeys return a lower case column name.
				 */
				for(ColumnMetaData colMetaData : columns)
				{
					if(colMetaData.getName().equalsIgnoreCase(pkColName))
					{
						primaryKeyColumns.add(colMetaData.getName());
					}
				}
			}
			rs.close();
			
			if((flags & INDICES) != 0)
			{
				if(!primaryKeyColumns.isEmpty())
				{
					indexMap.put(new IndexInfo("PRIMARY_KEY",IndexType.PRIMARY_KEY),
							primaryKeyColumns);
				}
				
				rs = meta.getIndexInfo(catalog,schema,tableName,false,true);
				
				while(rs.next())
				{
					String indexName = rs.getString("INDEX_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					if(indexName != null
						&& columnName != null
						&& !primaryKeyColumns.contains(columnName))
					{
						boolean unique = !rs.getBoolean("NON_UNIQUE");
						IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE
								: IndexType.NORMAL);
						Set<String> columnNames = indexMap.computeIfAbsent(info, k -> new HashSet<String>());
						columnNames.add(columnName);
					}
				}
				rs.close();
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				try
				{
					result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
							tableIdentity));
					if(result.next())
					{
						count = result.getInt(0);
					}
					result.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexMap.keySet())
		{
			fillIndices(indexMap, indices, i, indexInfo);
			i++;
		}
		
		return new TableMetaData(table,columns,indices,count);
	}
	
	private static void fillIndices(Map<IndexInfo, Set<String>> indexMap, Index[] indices, int i, IndexInfo indexInfo)
	{
		Set<String> columnList = indexMap.get(indexInfo);
		String[] indexColumns = columnList.toArray(new String[columnList.size()]);
		indices[i] = new Index(indexInfo.name, indexInfo.type,indexColumns);
	}
	
	private void fillColumnMetaData(
		String tableName,
		Map<String, Object> defaultValues,
		Map<String, DataType> dataTypeValues,
		Result result,
		ColumnMetaData[] columns,
		int i)
	{
		ColumnMetaData column = result.getMetadata(i);
		Object defaultValue = column.getDefaultValue();
		
		if(defaultValue == null && defaultValues.containsKey(column.getName()))
		{
			defaultValue = defaultValues.get(column.getName());
		}
		
		defaultValue = checkDefaultValue(defaultValue,column);
		
		// column.getName() is not always correct sometimes it returns
		// VARCHAR but it isnt. So take the result of the ResultSet above.
		if(column.getType() != DataType.VARCHAR)
		{
			
			columns[i] = new ColumnMetaData(
				tableName,
				column.getName(),
				column.getCaption(),
				column.getType(),
				column.getLength(),
				column.getScale(),
				defaultValue,
				column.isNullable(),
				column.isAutoIncrement());
			
		}
		else
		{
			columns[i] = new ColumnMetaData(
				tableName,
				column.getName(),
				column.getCaption(),
				dataTypeValues.get(column.getName()),
				column.getLength(),
				column.getScale(),
				defaultValue,
				column.isNullable(),
				column.isAutoIncrement());
			
		}
	}
	
	private static void fillDataTypeValues(Map<String, Object> defaultValues, Map<String, DataType> dataTypeValues, ResultSet rs)
		throws SQLException
	{
		final String columnName = rs.getString("COLUMN_NAME");
		final Object defaultValue = rs.getObject("COLUMN_DEF");
		defaultValues.put(columnName,defaultValue);
		final int dataTypeValue = rs.getInt("DATA_TYPE");
		final DataType dataType = DataType.get(dataTypeValue);
		
		dataTypeValues.put(columnName,dataType);
	}
	
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		return false;
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
}
