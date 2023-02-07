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

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

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


public class SQLiteJDBCMetaData extends JDBCMetaData
{
	public SQLiteJDBCMetaData(final SQLiteJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	private static void buildEntityRelationship(
		final EntityRelationshipModel model,
		final String pkTable,
		final String fkTable,
		final List<String> pkColumns,
		final List<String> fkColumns)
	{
		model.add(new EntityRelationship(
			new Entity(pkTable, pkColumns.toArray(new String[pkColumns.size()]), Cardinality.ONE),
			new Entity(fkTable, fkColumns.toArray(new String[fkColumns.size()]), Cardinality.MANY))
		);
		pkColumns.clear();
		fkColumns.clear();
	}
	
	private static void fillIndices(
		final Map<IndexInfo, Set<String>> indexMap,
		final Index[] indices,
		final int i,
		final IndexInfo indexInfo)
	{
		final Set<String> columnList = indexMap.get(indexInfo);
		final String[] indexColumns = columnList.toArray(new String[columnList.size()]);
		indices[i] = new Index(indexInfo.name, indexInfo.type, indexColumns);
	}
	
	private static void fillDataTypeValues(
		final Map<String, Object> defaultValues,
		final Map<String, DataType> dataTypeValues,
		final ResultSet rs)
		throws SQLException
	{
		final String columnName = rs.getString("COLUMN_NAME");
		final Object defaultValue = rs.getObject("COLUMN_DEF");
		defaultValues.put(columnName, defaultValue);
		final int dataTypeValue = rs.getInt("DATA_TYPE");
		final DataType dataType = DataType.get(dataTypeValue);
		
		dataTypeValues.put(columnName, dataType);
	}
	
	@Override
	protected String getSchema(final JDBCDataSource dataSource)
	{
		return null;
	}
	
	@Override
	protected String getCatalog(final JDBCDataSource dataSource)
	{
		return null;
	}
	
	@Override
	public EntityRelationshipModel getEntityRelationshipModel(
		final ProgressMonitor monitor,
		final TableInfo... tableInfos) throws DBException
	{
		monitor.beginTask("", tableInfos.length);
		
		final EntityRelationshipModel model = new EntityRelationshipModel();
		
		try
		{
			final List<String> tables = new ArrayList<>();
			for(final TableInfo table : tableInfos)
			{
				if(table.getType() == TableType.TABLE)
				{
					tables.add(table.getName());
				}
			}
			Collections.sort(tables);
			
			final ConnectionProvider connectionProvider = this.dataSource.getConnectionProvider();
			
			try(final Connection connection = connectionProvider.getConnection())
			{
				final DatabaseMetaData meta = connection.getMetaData();
				final String catalog = this.getCatalog(this.dataSource);
				final String schema = this.getSchema(this.dataSource);
				int done = 0;
				
				for(final String table : tables)
				{
					if(monitor.isCanceled())
					{
						break;
					}
					
					monitor.setTaskName(table);
					
					try(final ResultSet rs = meta.getExportedKeys(catalog, schema, table))
					{
						String pkTable = null;
						String fkTable = null;
						final List<String> pkColumns = new ArrayList<>();
						final List<String> fkColumns = new ArrayList<>();
						
						while(rs.next())
						{
							final short keySeq = rs.getShort("KEY_SEQ");
							
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
		catch(final SQLException e)
		{
			throw new DBException(this.dataSource, e);
		}
		
		monitor.done();
		
		return model;
	}
	
	@Override
	protected TableMetaData getTableMetaData(
		final JDBCConnection jdbcConnection, final DatabaseMetaData meta,
		final int flags, final TableInfo table) throws DBException, SQLException
	{
		final String catalog = this.getCatalog(this.dataSource);
		final String schema = this.getSchema(this.dataSource);
		
		final String tableName = table.getName();
		final Table tableIdentity = new Table(tableName, "META_DUMMY");
		
		final Map<String, Object> defaultValues = new HashMap<>();
		final Map<String, DataType> dataTypeValues = new HashMap<>();
		
		ResultSet rs = meta.getColumns(catalog, schema, tableName, null);
		while(rs.next())
		{
			fillDataTypeValues(defaultValues, dataTypeValues, rs);
		}
		rs.close();
		
		final SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		Result result = jdbcConnection.query(select);
		final int cc = result.getColumnCount();
		final ColumnMetaData[] columns = new ColumnMetaData[cc];
		
		for(int i = 0; i < cc; i++)
		{
			this.fillColumnMetaData(tableName, defaultValues, dataTypeValues, result, columns, i);
		}
		result.close();
		
		final Map<IndexInfo, Set<String>> indexMap = new LinkedHashMap<>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			final Set<String> primaryKeyColumns = new HashSet<>();
			rs = meta.getPrimaryKeys(catalog, schema, tableName);
			
			while(rs.next())
			{
				final String pkColName = rs.getString("COLUMN_NAME");
				/*
				 * fix for issue XDEVAPI-154
				 * the jdbc method #getPrimaryKeys return a lower case column name.
				 */
				for(final ColumnMetaData colMetaData : columns)
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
					indexMap.put(
						new IndexInfo("PRIMARY_KEY", IndexType.PRIMARY_KEY),
						primaryKeyColumns);
				}
				
				rs = meta.getIndexInfo(catalog, schema, tableName, false, true);
				
				while(rs.next())
				{
					final String indexName = rs.getString("INDEX_NAME");
					final String columnName = rs.getString("COLUMN_NAME");
					if(indexName != null
						&& columnName != null
						&& !primaryKeyColumns.contains(columnName))
					{
						final boolean unique = !rs.getBoolean("NON_UNIQUE");
						final IndexInfo info = new IndexInfo(indexName, unique ? IndexType.UNIQUE
							: IndexType.NORMAL);
						final Set<String> columnNames = indexMap.computeIfAbsent(info, k -> new HashSet<>());
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
				catch(final Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		final Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(final IndexInfo indexInfo : indexMap.keySet())
		{
			fillIndices(indexMap, indices, i, indexInfo);
			i++;
		}
		
		return new TableMetaData(table, columns, indices, count);
	}
	
	private void fillColumnMetaData(
		final String tableName,
		final Map<String, Object> defaultValues,
		final Map<String, DataType> dataTypeValues,
		final Result result,
		final ColumnMetaData[] columns,
		final int i)
	{
		final ColumnMetaData column = result.getMetadata(i);
		Object defaultValue = column.getDefaultValue();
		
		if(defaultValue == null && defaultValues.containsKey(column.getName()))
		{
			defaultValue = defaultValues.get(column.getName());
		}
		
		defaultValue = this.checkDefaultValue(defaultValue, column);
		
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
	
	@Override
	public boolean equalsType(final ColumnMetaData clientColumn, final ColumnMetaData dbColumn)
	{
		return false;
	}
	
	@Override
	protected void createTable(final JDBCConnection jdbcConnection, final TableMetaData table)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void addColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column, final ColumnMetaData columnBefore, final ColumnMetaData columnAfter)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void alterColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column, final ColumnMetaData existing) throws DBException, SQLException
	{
	}
	
	@Override
	protected void dropColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column) throws DBException, SQLException
	{
	}
	
	@Override
	protected void createIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void dropIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
		throws DBException, SQLException
	{
	}
}
