package load.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class CustomUDF extends UDF {

	private Text t = new Text();
	
	@Description(name="capitalize,cap", value="_FUNC_(str) - Returns str with all words capitalized", extended="Example:\n  > SELECT _FUNC_('facebook') FROM src LIMIT 1;\n  'Facebook'")
	public Text evaluate(Text s ) {
		if(s  == null) {
			return null;
		}
		this.t.set(StringUtils.capitalize(s.toString()));
		return this.t;
	}
}
