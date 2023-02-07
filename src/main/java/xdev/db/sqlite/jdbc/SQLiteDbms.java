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

import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class SQLiteDbms
	extends
	DbmsAdaptor.Implementation<SQLiteDbms, SQLiteDMLAssembler, SQLiteDDLMapper, SQLiteRetrospectionAccessor,
		SQLiteSyntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	public static final SQLiteSyntax SYNTAX = new SQLiteSyntax();
	/**
	 * The Constant MAX_VARCHAR_LENGTH.
	 */
	protected static final int MAX_VARCHAR_LENGTH = Integer.MAX_VALUE;
	protected static final char IDENTIFIER_DELIMITER = '"';
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public SQLiteDbms()
	{
		this(new SQLExceptionParser.Body());
	}
	
	/**
	 * @param sqlExceptionParser the sql exception parser
	 */
	public SQLiteDbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser, false);
		this.setRetrospectionAccessor(new SQLiteRetrospectionAccessor(this));
		this.setDMLAssembler(new SQLiteDMLAssembler(this));
		this.setSyntax(SYNTAX);
	}
	
	/**
	 * @see DbmsAdaptor#createConnectionInformation(String, int, String, String, String, String)
	 */
	@Override
	public SQLiteConnectionInformation createConnectionInformation(
		final String host,
		final int port, final String user, final String password, final String catalog, final String properties)
	{
		return new SQLiteConnectionInformation(catalog, properties, this);
	}
	
	/**
	 * HSQL does not support any means of calculating table columns selectivity as far as it is known.
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	/**
	 * @see DbmsAdaptor#assembleTransformBytes(byte[], StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return null;
	}
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor.Implementation#getRetrospectionAccessor()
	 */
	@Override
	public SQLiteRetrospectionAccessor getRetrospectionAccessor()
	{
		throw new RuntimeException("HSQL Retrospection not implemented yet!");
	}
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<SQLiteDbms> dbc)
	{
	}
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return true;
	}
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
