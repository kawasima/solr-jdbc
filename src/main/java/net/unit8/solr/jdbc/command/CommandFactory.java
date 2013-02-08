package net.unit8.solr.jdbc.command;

import java.sql.SQLException;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;


import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class CommandFactory {
	public static Command getCommand(Statement statement) throws SQLException{
		if (statement instanceof CreateTable) {
			return new CreateTableCommand((CreateTable)statement);
		}
		else if (statement instanceof Select) {
			return new SelectCommand((Select)statement);
		}
		else if (statement instanceof Drop) {
			return new DropCommand((Drop)statement);
		}
		else if (statement instanceof Update) {
			return new UpdateCommand((Update)statement);
		}
		else if (statement instanceof Insert) {
			return new InsertCommand((Insert)statement);
		}
		else if (statement instanceof Delete) {
			return new DeleteCommand((Delete)statement);
		}
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}
}
