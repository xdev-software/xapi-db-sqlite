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




import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class SQLiteJDBCDataSource extends JDBCDataSource<SQLiteJDBCDataSource, SQLiteDbms>
{
	public SQLiteJDBCDataSource()
	{
		super(new SQLiteDbms());
	}
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{
			CATALOG.clone(),
			URL_EXTENSION.clone(),
			IS_SERVER_DATASOURCE.clone(),
			SERVER_URL.clone(),
			AUTH_KEY.clone()
		};
	}
	
	
	@Override
	protected SQLiteConnectionInformation getConnectionInformation()
	{
		return new SQLiteConnectionInformation(getCatalog(),getUrlExtension(),getDbmsAdaptor());
	}
	
	
	@Override
	public SQLiteJDBCConnection openConnectionImpl() throws DBException
	{
		return new SQLiteJDBCConnection(this);
	}
	
	
	@Override
	public SQLiteJDBCMetaData getMetaData() throws DBException
	{
		return new SQLiteJDBCMetaData(this);
	}
	
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
