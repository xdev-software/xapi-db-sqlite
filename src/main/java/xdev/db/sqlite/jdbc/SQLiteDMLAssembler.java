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

import static com.xdev.jadoth.sqlengine.internal.QueryPart.ASEXPRESSION;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;

import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;
import com.xdev.jadoth.sqlengine.internal.SqlExpression;
import com.xdev.jadoth.sqlengine.internal.SqlFunctionMOD;


public class SQLiteDMLAssembler extends StandardDMLAssembler<SQLiteDbms>
{
	public SQLiteDMLAssembler(final SQLiteDbms dbms)
	{
		super(dbms);
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @see StandardDMLAssembler#assembleSELECT(SELECT, StringBuilder, int, int, String, String)
	 */
	@Override
	protected StringBuilder assembleSELECT(
		final SELECT query, final StringBuilder sb,
		final int indentLevel, final int flags, final String clauseSeperator,
		final String newLine)
	{
		indent(sb, indentLevel, isSingleLine(flags)).append(query.keyword());
		
		this.assembleSelectDISTINCT(query, sb, indentLevel, flags);
		this.assembleSelectItems(query, sb, flags, indentLevel, newLine);
		this.assembleSelectSqlClauses(query, sb, indentLevel, flags | ASEXPRESSION, clauseSeperator, newLine);
		this.assembleAppendSELECTs(query, sb, indentLevel, flags, clauseSeperator, newLine);
		this.assembleSelectRowLimit(query, sb, flags, clauseSeperator, newLine, indentLevel);
		
		return sb;
	}
	
	/**
	 * @see StandardDMLAssembler#assembleSelectRowLimit(SELECT, StringBuilder, int, String, String, int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(
		final SELECT query, final StringBuilder sb,
		final int flags, final String clauseSeperator, final String newLine,
		final int indentLevel)
	{
		final Integer offset = query.getOffsetSkipCount();
		final Integer limit = query.getFetchFirstRowCount();
		
		if(offset != null && limit != null)
		{
			sb.append(newLine)
				.append(clauseSeperator)
				.append("LIMIT ")
				.append(limit)
				.append(" OFFSET ")
				.append(offset);
		}
		else if(limit != null)
		{
			sb.append(newLine)
				.append(clauseSeperator)
				.append("LIMIT ")
				.append(limit);
		}
		return sb;
	}
	
	@Override
	public void assembleExpression(
		final SqlExpression expression, final StringBuilder sb,
		final int indentLevel, final int flags)
	{
		
		if(expression instanceof SqlFunctionMOD)
		{
			sb.append(((SqlFunctionMOD)expression).getParameters()[0])
				.append(" % ")
				.append(((SqlFunctionMOD)expression).getParameters()[1]);
		}
		else
		{
			super.assembleExpression(expression, sb, indentLevel, flags);
		}
	}
}
