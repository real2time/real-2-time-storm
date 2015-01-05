package util;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.xml.sax.InputSource;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.w3c.dom.Node;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Aylien2 {


	public class Language {
	  private String lang;
	  private Double confidence;

	  public String getLang() {
	    return lang;
	  }
	  public void setLang(String lang) {
	    this.lang = lang;
	  }
	  public Double getConfidence() {
	    return confidence;
	  }
	  public void setConfidence(Double confidence) {
	    this.confidence = confidence;
	  }
	} 

	public class Sentiment {
	  private String polarity;
	  private Double polarityConfidence;

	  public String getPolarity() {
	    return polarity;
	  }
	  public void setPolarity(String polarity) {
	    this.polarity = polarity;
	  }

	  public Double getPolarityConfidence() {
	    return polarityConfidence;
	  }
	  public void setPolarityConfidence(Double confidence) {
	    this.polarityConfidence = confidence;
	  }
	}

	
	  private String APPLICATION_ID;
	  private String APPLICATION_KEY;

	  public Aylien2(String application_id, String application_key) {
		  
			this.APPLICATION_ID = application_id;
			this.APPLICATION_KEY = application_key;
						  
		  }
	  
	  public Aylien2(String json) {
		  
		Object obj=JSONValue.parse(json);
		JSONArray array=(JSONArray)obj;
		JSONObject obj2=(JSONObject)array.get(0);
		
		this.APPLICATION_ID = obj2.get("application_id").toString();
		this.APPLICATION_KEY = obj2.get("application_key").toString();
		

		  
	  }

	  public Language getLanguage(String text) {
	    final Map<String, String> parameters;
	    parameters = new HashMap<String, String>();
	    parameters.put("text", text);
	    Document doc = callAPI("language", parameters);
	    Language language = new Language();
	    NodeList nodeList = doc.getElementsByTagName("lang");
	    Node langNode = nodeList.item(0);
	    nodeList = doc.getElementsByTagName("confidence");
	    Node confNode = nodeList.item(0);
	    language.setLang(langNode.getTextContent());
	    language.setConfidence(Double.parseDouble(confNode.getTextContent()));

	    return language;
	  }

	  public Sentiment getSentiment(String text) {
	    final Map<String, String> parameters;
	    parameters = new HashMap<String, String>();
	    parameters.put("text", text);
	    parameters.put("mode", "tweet");
	    Document doc = callAPI("sentiment", parameters);
	    Sentiment sentiment = new Sentiment();
	    NodeList nodeList = doc.getElementsByTagName("polarity");
	    Node polarityNode = nodeList.item(0);
	    nodeList = doc.getElementsByTagName("polarity_confidence");
	    Node confNode = nodeList.item(0);
	    sentiment.setPolarity(polarityNode.getTextContent());
	    sentiment.setPolarityConfidence(Double.parseDouble(confNode.getTextContent()));

	    return sentiment;
	  }
	  
	  public Document getEntity(String text) {
		    final Map<String, String> parameters;
		    parameters = new HashMap<String, String>();
		    parameters.put("text", text);
		    Document doc = callAPI("entities", parameters);
		    NodeList nodeList = doc.getElementsByTagName("text");
		    System.out.println(nodeList.toString());
		    /*Sentiment sentiment = new Sentiment();
		    NodeList nodeList = doc.getElementsByTagName("polarity");
		    Node polarityNode = nodeList.item(0);
		    nodeList = doc.getElementsByTagName("polarity_confidence");
		    Node confNode = nodeList.item(0);
		    sentiment.setPolarity(polarityNode.getTextContent());
		    sentiment.setPolarityConfidence(Double.parseDouble(confNode.getTextContent()));
*/
		    return doc;
		  }

	  public Document callAPI(String endpoint, Map<String, String> parameters) {
	    URL url;
	    HttpURLConnection connection = null;

	    try {
	      String queryString = "";
	      StringBuilder sb = new StringBuilder();
	      for (Map.Entry<String, String> e: parameters.entrySet()) {
	        if (sb.length() > 0) { sb.append('&'); }
	        sb.append(URLEncoder.encode(e.getKey(), "UTF-8")).append('=')
	          .append(URLEncoder.encode(e.getValue(), "UTF-8"));
	      }
	      queryString = sb.toString();
	      url = new URL("https://api.aylien.com/api/v1/" + endpoint);
	      connection = (HttpURLConnection)url.openConnection();
	      connection.setRequestMethod("POST");
	      connection.setRequestProperty(
	          "Content-Type", "application/x-www-form-urlencoded");
	      connection.setRequestProperty(
	          "Content-Length", Integer.toString(queryString.getBytes().length));
	      connection.setRequestProperty("Accept", "text/xml");
	      connection.setRequestProperty(
	          "X-AYLIEN-TextAPI-Application-ID", APPLICATION_ID);
	      connection.setRequestProperty(
	          "X-AYLIEN-TextAPI-Application-Key", APPLICATION_KEY);
	      connection.setDoInput(true);
	      connection.setDoOutput(true);

	      DataOutputStream dos = new DataOutputStream(connection.getOutputStream());
	      dos.writeBytes(queryString);
	      dos.flush();
	      dos.close();

	      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	      DocumentBuilder builder = factory.newDocumentBuilder();
	      InputSource xis = new InputSource(connection.getInputStream());

	      return builder.parse(xis);
	    } catch (Exception e) {
	      e.printStackTrace();
	      return null;
	    } finally {
	    }
	  }

	}
