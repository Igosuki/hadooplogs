//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 in JDK 6 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2011.11.17 at 11:57:44 AM CET 
//


package load.hadoop.conf.xml;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{}assemblies"/>
 *         &lt;element ref="{}expression"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "assemblies",
    "expression"
})
@XmlRootElement(name = "outconf")
public class Outconf {

    @XmlElement(required = true)
    protected Assemblies assemblies;
    @XmlElement(required = true)
    protected String expression;

    /**
     * Gets the value of the assemblies property.
     * 
     * @return
     *     possible object is
     *     {@link Assemblies }
     *     
     */
    public Assemblies getAssemblies() {
        return assemblies;
    }

    /**
     * Sets the value of the assemblies property.
     * 
     * @param value
     *     allowed object is
     *     {@link Assemblies }
     *     
     */
    public void setAssemblies(Assemblies value) {
        this.assemblies = value;
    }

    /**
     * Gets the value of the expression property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getExpression() {
        return expression;
    }

    /**
     * Sets the value of the expression property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setExpression(String value) {
        this.expression = value;
    }

}
