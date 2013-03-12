package net.unit8.solr.jdbc.expression;

import org.apache.commons.lang.StringUtils;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.SolrType;
import net.unit8.solr.jdbc.value.SolrValue;
import net.unit8.solr.jdbc.value.ValueNull;

public class Parameter implements Item{
	private SolrValue value;
	private int index;
	private boolean needsLikeEscape;
	private String likeEscapeChar = "%";
	private Expression targetColumn;

	public Parameter(int index) {
		this.index = index;
	}

	public void setValue(SolrValue value) {
		this.value = value;
	}

	public SolrValue getValue() {
		if(value == null) {
			return ValueNull.INSTANCE;
		}
		return value;
	}

	public void setColumn(Expression column) {
		this.targetColumn = column;
	}
	public SolrType getType() {
		if (value != null) {
			return value.getType();
		}
		return SolrType.UNKNOWN;
	}

	public String getQueryString() {
		if (value != null) {
			if (needsLikeEscape) {
				String strValue = value.getString();
				if (targetColumn != null && targetColumn.getType() == SolrType.TEXT) {
					if (strValue.startsWith(likeEscapeChar)
							&& strValue.endsWith(likeEscapeChar)) {
						strValue = StringUtils.strip(strValue, likeEscapeChar);
					}
				}
				if (strValue.startsWith(likeEscapeChar)) {
					// TODO 専用のメッセージを作る
					throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
							"この型では中間一致・後方一致検索はできません");
				}
				return strValue.replaceAll(likeEscapeChar, "*");
			} else {
				return value.getQueryString();
			}
		}
		return "";
	}
	public int getIndex() {
		return index;
	}

	public void setNeedsLikeEscape() {
		this.needsLikeEscape = true;
	}

	public void setLikeEscapeChar(String likeEscapeChar) {
		this.likeEscapeChar = likeEscapeChar;
	}

	@Override
	public String toString() {
		return getQueryString();
	}
}
