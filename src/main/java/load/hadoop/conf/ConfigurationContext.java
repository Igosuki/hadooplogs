package load.hadoop.conf;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.el.ExpressionFactory;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import load.hadoop.conf.xml.Configuration;
import load.hadoop.conf.xml.Constant;
import load.hadoop.conf.xml.Filename;
import load.hadoop.conf.xml.Parse;
import load.hadoop.conf.xml.Parseconf;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.util.SimpleContext;

public class ConfigurationContext {

	private static final Log LOG = LogFactory.getLog(ConfigurationContext.class);
	
	private String buildString;
	
	private boolean resolved;
	
	private SimpleContext context;
	
	private ExpressionFactory ef;
	
	private Configuration conf;
	
	public ConfigurationContext(String filename, InputStream confInput) {
		try {
			JAXBContext xmlConf = JAXBContext.newInstance("load.hadoop.conf.xml");
			Unmarshaller um = xmlConf.createUnmarshaller();
			um.unmarshal(confInput);
		} catch (JAXBException e) {
			LOG.info("Failed to build configuration");
		}
		ef = new ExpressionFactoryImpl();
		parseFileName(filename);
		final List<Constant> constant = conf.getParseconf().getConstants().getConstant();
		for (Constant c : constant) {
			context.setVariable(c.getName(), ef.createValueExpression(c.getValue(), String.class));
		}
	}
	
	public void put (String key, Object value, Class clazz) {
		context.setVariable(key, ef.createValueExpression(value, clazz));
	}
	
	public boolean isResolved() {
		return this.resolved;
	}
	
	public void setResolved(boolean resolved) {
		this.resolved = resolved;
	}
	public void parseFileName(String fileName) {
		parseText(conf.getParseconf().getFilename().getParse(), fileName);
	}
	public void parseContent(String text) {
		parseText(conf.getParseconf().getContent().getParse(), text);
	}
	public void parseText(Parse parseconf, String text) {
		if(!StringUtils.isBlank(parseconf.getName())) {
			put(parseconf.getName(), text, String.class);
		}
		if(!StringUtils.isBlank(parseconf.getDelim())) {
			String[] split = splitByDelim(parseconf, text);
			for (Parse parse : parseconf.getSubparses().getParse()) {
				
			}
		}
	}

	private String[] splitByDelim(Parse parseconf, String text) {
		if (!StringUtils.isBlank(parseconf.getDelim())) {
			if (parseconf.getDelim().length() == 1) {
				switch(parseconf.getDelim().charAt(0)){
					case '.' : 
					case '$' : 
					case '{' :
					case '}' :
					case '[' :
					case ']' :
					case '*' : 
					case '^' : 
						return text.split("\\"+parseconf.getDelim());
					default : 
						return text.split(parseconf.getDelim());
				}
			}
			return text.split(parseconf.getDelim());
		}
		return null;
	}
	
}
