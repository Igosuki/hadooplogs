package load.hadoop.conf;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import load.hadoop.conf.xml.Assemblies;
import load.hadoop.conf.xml.Assembly;
import load.hadoop.conf.xml.Configuration;
import load.hadoop.conf.xml.Constant;
import load.hadoop.conf.xml.Filename;
import load.hadoop.conf.xml.Parse;
import load.hadoop.conf.xml.Parseconf;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.TreeValueExpression;
import de.odysseus.el.util.SimpleContext;

public class ConfigurationContext {

	private static final Log LOG = LogFactory.getLog(ConfigurationContext.class);
	
	private String buildString;
	
	private boolean resolved;
	
	private SimpleContext context;
	
	private ExpressionFactoryImpl ef;
	
	private Configuration conf;
	
	private HashMap<Parse, Range[]> ranges = new HashMap<Parse, Range[]>();
	
	public ConfigurationContext(String filename, InputStream confInput) {
		try {
			JAXBContext xmlConf = JAXBContext.newInstance("load.hadoop.conf.xml");
			Unmarshaller um = xmlConf.createUnmarshaller();
			this.conf = (Configuration) um.unmarshal(confInput);
			optimizeConf(this.conf);
		} catch (JAXBException e) {
			LOG.info("Failed to build configuration");
		}
		ef = new ExpressionFactoryImpl();
		context = new SimpleContext();
		parseFileName(filename);
		final List<Constant> constant = conf.getParseconf().getConstants().getConstant();
		for (Constant c : constant) {
			context.setVariable(c.getName(), ef.createValueExpression(c.getValue(), String.class));
		}
		
	}
	
	public String flush() {
		for (Assembly ass : conf.getOutconf().getAssemblies().getAssembly()) {
			TreeValueExpression tve = ef.createValueExpression(context, ass.getExpression(), String.class);
			put(ass.getName(), tve.getValue(context), String.class);
		}
		TreeValueExpression tve = ef.createValueExpression(context, conf.getOutconf().getExpression(), String.class);
		return (String) tve.getValue(context);
	}
	
	public void put (String key, Object value, Class clazz) {
		context.setVariable(key, ef.createValueExpression(value, clazz));
	}

	/**
	 * Parse the filename 
	 */
	public void parseFileName(String fileName) {
		parseText(conf.getParseconf().getFilename().getParse(), fileName);
	}
	
	/**
	 * Parse text content
	 */
	public void parseContent(String text) {
		parseText(conf.getParseconf().getContent().getParse(), text);
	}
	
	/**
	 * Parse text with a parse conf 
	 */
	public void parseText(Parse parseconf, String text) {
		if(!StringUtils.isBlank(parseconf.getName())) {
			if(!StringUtils.isBlank(text)) {
				put(parseconf.getName(), text, String.class);
			} else {
				put(parseconf.getName(), "null", String.class);
			}
		}
		String[] split = null;
		if(!StringUtils.isBlank(parseconf.getDelim())) {
			split = splitByDelim(parseconf, text);
		} else {
			final Range[] currRanges = this.ranges.get(parseconf);
			if(currRanges != null){
				split = new String[currRanges.length];
				int k = 0;
				StringBuilder sb = new StringBuilder(5);
				for (int i = 0; i < currRanges.length; i++) { 
					k = currRanges[i].getStart();
					while(k < text.length() && k <= currRanges[i].getEnd()) {
						sb.append(text.charAt(k));
						k++;
					}
					split[i] = sb.toString();
					sb.setLength(0);
				}
			}
		}
		if(split != null) {
			if(parseconf.getSubparses().isNatural()) {
				for (int i = 0; i < parseconf.getSubparses().getParse().size() && i < split.length; i++) {
					parseText(parseconf.getSubparses().getParse().get(i), split[i]);
				}
			} else {
				for (Parse parse : parseconf.getSubparses().getParse()) {
					if(parse.getIndex() != null && parse.getIndex().intValue() < split.length) {
						parseText(parse, split[parse.getIndex().intValue()]);
					}
				}
			}
		}
	}

	/**
	 * Split by a delimitor
	 */
	private String[] splitByDelim(Parse parse, String text) {
		return text.split(parse.getDelim());
	}
	
	/**
	 * Optimize a configuration tree for memory usage
	 */
	public void optimizeConf(Configuration conf) {
		optimizeParse(conf.getParseconf().getContent().getParse());
		optimizeParse(conf.getParseconf().getFilename().getParse());
	}
	
	/**
	 * Optimize a parse tree for memory usage
	 */
	public void optimizeParse(Parse parse) {
		if (!StringUtils.isBlank(parse.getDelim())) {
			if(!parse.isRegex()) {
				parse.setDelim(escape(parse.getDelim()));
			}
		} else if(!StringUtils.isBlank(parse.getRanges())) {
			try {
				if(parse.getRanges().charAt(0) != '[' 
						|| parse.getRanges().charAt(parse.getRanges().length() -1) != ']') {
					throw new NumberFormatException("Invalid range string");
				}
				String[] rangessplit = parse.getRanges().substring(1, parse.getRanges().length() - 1).split(",");
				Range[] ranges = new Range[rangessplit.length];
				for (int i = 0; i < rangessplit.length; i++) {
					String[] rangesplit = rangessplit[i].split("\\.\\.");
					if(rangesplit.length == 2) {
						ranges[i] = new Range(Integer.valueOf(rangesplit[0]), Integer.valueOf(rangesplit[1]));
					} else {
						throw new NumberFormatException("Invalid range format");
					}
				}
				this.ranges.put(parse, ranges);
			} catch (NumberFormatException e) {
				LOG.debug("Invalid range : " + e.getMessage());
			}
		}
		if(parse.getSubparses() != null) {
			for (Parse subparse : parse.getSubparses().getParse()) {
				optimizeParse(subparse);
			}
		}
	}
	
	/**
	 * Escape all special characters in a string
	 */
	public String escape(String str) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < str.length(); i++) {
			switch(str.charAt(i)){
				case '.' : 
				case '$' : 
				case '{' :
				case '}' :
				case '[' :
				case ']' :
				case '*' : 
				case '^' : 
					sb.append(escapeChar(str.charAt(i)));
					break;
				default : 
					sb.append(str.charAt(i));
					break;
			}
		}
		return sb.toString();
	}

	/**
	 * Escape a single char
	 * @return the escaped char in a string
	 */
	public String escapeChar(char c) {
		return "\\"+c;
	}
	
	public boolean isResolved() {
		return this.resolved;
	}
	
	public void setResolved(boolean resolved) {
		this.resolved = resolved;
	}
}
