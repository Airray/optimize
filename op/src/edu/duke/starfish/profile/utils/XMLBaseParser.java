package edu.duke.starfish.profile.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public abstract class XMLBaseParser<T> {
	private static final Log LOG = LogFactory.getLog(XMLBaseParser.class);

	public T importXML(File inputFile) {
		InputStream in = null;
		Object t = null;
		try {
			in = new FileInputStream(inputFile);
			t = importXML(in);
		} catch (FileNotFoundException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		} finally {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
			}
		}
		return t;
	}

	public T importXML(InputStream in) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setIgnoringComments(true);
		Document doc = null;
		Object t = null;
		try {
			doc = dbf.newDocumentBuilder().parse(in);
			t = importXML(doc);
		} catch (ParserConfigurationException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		} catch (SAXException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		} catch (IOException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		}

		return t;
	}

	public void exportXML(T object, File outFile) {
		PrintStream out = null;
		try {
			out = new PrintStream(outFile);
			exportXML(object, out);
		} catch (FileNotFoundException e) {
			LOG.error("Unable to export XML file", e);
		} finally {
			if (out != null)
				out.close();
		}
	}

	public void exportXML(T object, PrintStream out) {
		Document doc = null;
		try {
			System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
					"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
					.newDocument();
		} catch (ParserConfigurationException e) {
			LOG.error("Unable to export XML file", e);
		}

		exportXML(object, doc);

		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(out);
		try {
			Transformer transformer = TransformerFactory.newInstance()
					.newTransformer();

			transformer.setOutputProperty("indent", "yes");
			transformer.transform(source, result);
		} catch (TransformerConfigurationException e) {
			LOG.error("Unable to export XML file", e);
		} catch (TransformerException e) {
			LOG.error("Unable to export XML file", e);
		}
	}

	protected abstract T importXML(Document paramDocument);

	protected abstract void exportXML(T paramT, Document paramDocument);
}