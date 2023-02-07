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

import java.io.File;

import com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation;

import xdev.db.ConnectionInformation;


public class SQLiteConnectionInformation extends ConnectionInformation<SQLiteDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public SQLiteConnectionInformation(
		final String database, final String urlExtension,
		final SQLiteDbms dbmsAdaptor)
	{
		super("", 0, "", "", database, urlExtension, dbmsAdaptor);
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	
	/**
	 * Gets the database.
	 *
	 * @return the database
	 */
	public String getDatabase()
	{
		return this.getCatalog();
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	
	/**
	 * Sets the database.
	 *
	 * @param database the database to set
	 */
	public void setDatabase(final String database)
	{
		this.setCatalog(database);
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @see DbmsConnectionInformation#createJdbcConnectionUrl()
	 */
	@Override
	public String createJdbcConnectionUrl()
	{
		String db = this.getDatabase();
		File file = new File(db).getAbsoluteFile();
		if(!file.exists())
		{
			final String projectHome = System.getProperty("project.home", null);
			if(projectHome != null && projectHome.length() > 0)
			{
				file = new File(new File(projectHome), db);
				db = file.getAbsolutePath();
			}
		}
		
		final String url = "jdbc:sqlite:" + db;
		return this.appendUrlExtension(url);
	}
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#getJdbcDriverClassName()
	 */
	@Override
	public String getJdbcDriverClassName()
	{
		return "org.sqlite.JDBC";
	}
}
