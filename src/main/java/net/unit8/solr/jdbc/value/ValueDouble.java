package net.unit8.solr.jdbc.value;



public class ValueDouble extends SolrValue {

	private static final double DOUBLE_ZERO = 0.0;
    private static final double DOUBLE_ONE = 1.0;
    private static final ValueDouble ZERO = new ValueDouble(DOUBLE_ZERO);
    private static final ValueDouble ONE = new ValueDouble(DOUBLE_ONE);
    private static final ValueDouble NAN = new ValueDouble(Double.NaN);

    private final double value;

    private ValueDouble(double value) {
    	this.value = value;
    }

    @Override
	public SolrType getType() {
        return SolrType.DOUBLE;
    }

	@Override
	public Object getObject() {
		return Double.valueOf(value);
	}

	@Override
	public String getQueryString() {
		return getString();
	}

	@Override
	public String getString() {
		return String.valueOf(value);
	}

    @Override
	public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    public static ValueDouble get(double d) {
        if (DOUBLE_ZERO == d) {
            return ZERO;
        } else if (DOUBLE_ONE == d) {
            return ONE;
        } else if (Double.isNaN(d)) {
            return NAN;
        }
        return new ValueDouble(d);
    }

}
