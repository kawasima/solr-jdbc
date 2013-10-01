package net.unit8.solr.jdbc.expression;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.SolrType;
import net.unit8.solr.jdbc.value.SolrValue;
import net.unit8.solr.jdbc.value.ValueNull;
import org.apache.solr.client.solrj.util.ClientUtils;

public class Parameter implements Item {
    private static final String SQL_WILDCARD_CHARS = "%_％＿";

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
				if (targetColumn != null && targetColumn.getType() == SolrType.TEXT) {
                    StringBuilder sb = processWildcard(value.getString());
                    // If the parameter matched partially in the middle,
                    // trim the first & last wildcards for the syntax of proper solr query.
                    if (sb.charAt(sb.length() - 1) == '*') {
                        if (sb.charAt(0) == '*')
                            sb.deleteCharAt(0);
                        sb.deleteCharAt(sb.length() - 1);
                        return sb.toString();
                    } else {
                        throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
                                "In TEXT type, supports partial matching in the middle of words only.");
                    }
				} else if (targetColumn.getType() == SolrType.STRING) {
                    StringBuilder sb = processWildcard(value.getString());
                    if (sb.charAt(0) != '*' && sb.charAt(sb.length() - 1) == '*') {
                        return sb.toString();
                    } else {
                        throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
                                "In STRING type, supports partial matching in the beginning of words only.");
                    }
				} else {
                    throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
                            "Like is not supported in this type.");
                }
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

    private StringBuilder processWildcard(String value) {
        String escapedValue = value
                .replaceAll("\\*", "\\\\*")
                .replaceAll("(?<!\\" + likeEscapeChar + ")[" + SQL_WILDCARD_CHARS + "]", "*")
                .replaceAll("\\" + likeEscapeChar + "([" + SQL_WILDCARD_CHARS + "])", "$1");

        String[] tokens = escapedValue.split("(?<!\\\\)\\*", -1);
        StringBuilder sb = new StringBuilder(escapedValue.length() + 20);
        for (int i=0; i < tokens.length; i++) {
            sb.append(ClientUtils.escapeQueryChars(tokens[i].replaceAll("\\\\\\*", "*")));
            if (i < tokens.length -1)
                sb.append("*");
        }
        return sb;
    }
}
