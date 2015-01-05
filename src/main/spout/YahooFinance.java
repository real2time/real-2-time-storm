package spout;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;


public class YahooFinance extends BaseRichSpout {

    private static Logger logger = Logger.getLogger(YahooFinance.class);

    SpoutOutputCollector outputCollector;
    Random _rand;
    protected ArrayList<String> stocks;

    // http://cliffngan.net/a/13
    protected String yahooResponseFormat = "sl1d1t1cv";

    // "http://finance.yahoo.com/d/quotes.csv?s=DATA&f=snl1d1t1cv&e=.csv"
    protected String yahooService = "http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s&e=.csv";

    // Sleep time when between calles to the Spout
    protected int sleep = 1000;
    String[] fields;
    
	public YahooFinance(String json){
		
		Object obj=JSONValue.parse(json);
		JSONArray array=(JSONArray)obj;
		JSONObject obj2=(JSONObject)array.get(0);
		this.stocks = Lists.newArrayList();
		this.stocks.add(obj2.get("stock").toString());
		 
		/*
		JSONArray stocksJson=(JSONArray)obj2.get("stocks");
		ArrayList<String> stocksTmp = Lists.newArrayList();;
		for (int i = 0; i < stocksJson.size(); i++) {
			stocksTmp.add((String) stocksJson.get(i));
		}*/
		
		JSONArray fieldsJson=(JSONArray)obj2.get("emitedFields");
		String[] fieldsTmp = new String[fieldsJson.size()];
		//if (fieldsJson.size() == 0) 
		for (int i = 0; i < fieldsJson.size(); i++) {
			fieldsTmp[i] = (String) fieldsJson.get(i);
		}
		
		//this.stocks = stocksTmp; 
		this.fields = fieldsTmp;
		
	}
	
	public YahooFinance(){
        // Initialize the stocks that need to be polled
        stocks = Lists.newArrayList();
        stocks.add("FB");
        stocks.add("DATA");
        stocks.add("AAPL");
        
	}
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(sleep);
        int idx = new Double(Math.random() * stocks.size()).intValue(); // generate random index
        String stock = stocks.get(idx);

        String query = String.format(yahooService, stock, yahooResponseFormat);
        try {

            URL url = new URL(query);
            String response = IOUtils.toString(url.openStream()).replaceAll("\"", "");
            String[] lSplit = response.split(",");
            String stockName = lSplit[0];
            double stockPrice = Double.parseDouble(lSplit[1]);
            
            String date = lSplit[2];
            String time = lSplit[3];
            
            SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy hh:mma");
            Date parsedDate = df.parse(date  + " "  + time);
            
//			  fake values (handy when stock market is closed...)
//            stockPrice = Math.random()*100;
//            parsedDate = new Date();

            outputCollector.emit(new Values(stockName, stockPrice, parsedDate));
        } catch (MalformedURLException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (ParseException e) {
        	logger.error(e.getMessage(), e);
		}
    }


    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields(this.fields));
    	//declarer.declare(new Fields("stock", "value", "date"));
    }

}