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
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import xdev.db.DBException;
import xdev.db.QueryInfo;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCResult;
import xdev.db.sql.SELECT;
import xdev.io.ByteHolder;
import xdev.io.CharHolder;
import xdev.vt.XdevBlob;
import xdev.vt.XdevClob;


public class SQLiteJDBCConnection extends JDBCConnection<SQLiteJDBCDataSource, SQLiteDbms>
{
	public SQLiteJDBCConnection(final SQLiteJDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	/**
	 * Original method had a bug (see issue 13769). We are solving this here. If you have any problems using this
	 * adapter, try to delete JDBCResult(int, scale, final ResultSet rs)
	 */
	@Override
	public JDBCResult query(final String sql, final Integer offset, final Integer maxRowCount, final Object... params)
		throws DBException
	{
		try
		{
			final ResultSet rs = this.queryJDBC(sql, params);
			
			final JDBCResult result;
			if((offset != null || maxRowCount != null)
				&& !this.gateway.getDbmsAdaptor().supportsOFFSET_ROWS())
			{
				result = new JDBCResult(rs, offset != null ? offset : 0, 0, maxRowCount);
			}
			else
			{
				result = new JDBCResult(0, rs);
			}
			result.setDataSource(this.dataSource);
			return result;
		}
		catch(final DBException e)
		{
			throw e;
		}
		catch(final Exception e)
		{
			throw new DBException(this.dataSource, e);
		}
	}
	
	/**
	 * This Method is fix for issue 13769.
	 *
	 * @param select an {@link SELECT} statement to be sent to the database, typically a static SQL <code>SELECT</code>
	 *               statement
	 * @param params the parameter values for the given <code>select</code>
	 */
	@Override
	public JDBCResult query(final SELECT select, final Object... params) throws DBException
	{
		this.decorateDelegate(select, this.gateway);
		
		final Integer offset = select.getOffsetSkipCount();
		Integer limit = select.getFetchFirstRowCount();
		
		if(!this.gateway.getDbmsAdaptor().supportsOFFSET_ROWS() && offset != null && offset > 0
			&& limit != null && limit > 0)
		{
			limit += offset;
			select.FETCH_FIRST(limit);
		}
		
		final String sql = select.toString();
		final JDBCResult result = this.query(sql, offset, limit, params);
		result.setQueryInfo(new QueryInfo(select, params));
		return result;
	}
	
	/**
	 * This Method is fix for issue 13769.
	 *
	 * @param sql    an SQL String to be sent to the database
	 * @param params the parameter values for the given <code>sql</code>
	 */
	@Override
	public JDBCResult query(final String sql, final Object... params) throws DBException
	{
		return this.query(sql, null, null, params);
	}
	
	@Override
	protected void prepareParams(final Connection connection, final Object[] params) throws DBException
	{
		if(params == null)
		{
			return;
		}
		
		for(int i = 0; i < params.length; i++)
		{
			final Object param = params[i];
			if(param == null)
			{
				continue;
			}
			
			if(param instanceof XdevBlob)
			{
				params[i] = ((XdevBlob)param).toJDBCBlob();
			}
			else if(param instanceof ByteHolder)
			{
				params[i] = ((ByteHolder)param).toByteArray();
			}
			else if(param instanceof XdevClob)
			{
				// transform clob to string
				params[i] = param.toString();
			}
			else if(param instanceof CharHolder)
			{
				params[i] = ((CharHolder)param).toCharArray();
			}
			else if(param instanceof Calendar)
			{
				params[i] = new Timestamp(((Calendar)param).getTimeInMillis());
			}
			else if(param instanceof java.util.Date)
			{
				params[i] = new Timestamp(((java.util.Date)param).getTime());
			}
		}
	}
	
	@Override
	public void createTable(
		final String tableName, final String primaryKey, final Map<String, String> columnMap,
		final boolean isAutoIncrement, final Map<String, String> foreignKeys) throws Exception
	{
		
		if(!columnMap.containsKey(primaryKey))
		{
			columnMap.put(primaryKey, "INTEGER"); //$NON-NLS-1$
		}
		
		final StringBuffer createStatement = new StringBuffer("CREATE TABLE IF NOT EXISTS " + tableName //$NON-NLS-1$
			+ "(" + primaryKey + " " + columnMap.get(primaryKey)
			+ " PRIMARY KEY,"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		int i = 1;
		for(final String keySet : columnMap.keySet())
		{
			if(!keySet.equals(primaryKey))
			{
				if(i < columnMap.size())
				{
					createStatement.append(keySet)
						.append(" ")
						.append(columnMap.get(keySet))
						.append(","); //$NON-NLS-1$ //$NON-NLS-2$
				}
				else
				{
					createStatement.append(keySet)
						.append(" ")
						.append(columnMap.get(keySet))
						.append(")"); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			i++;
		}
		
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + createStatement); //$NON-NLS-1$
		}
		
		final Connection connection = super.getConnection();
		final Statement statement = connection.createStatement();
		try
		{
			statement.execute(createStatement.toString());
		}
		catch(final Exception e)
		{
			throw e;
		}
		finally
		{
			statement.close();
			connection.close();
		}
	}
}
