/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class Twitter extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	String[] fields;

	public Twitter(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
	}
	
	public Twitter(String json){
		
		Object obj=JSONValue.parse(json);
		JSONArray array=(JSONArray)obj;
		JSONObject obj2=(JSONObject)array.get(0);
				
		this.consumerKey = obj2.get("consumer_key").toString();
		this.consumerSecret = obj2.get("consumer_secret").toString();
		this.accessToken = obj2.get("access_token").toString();
		this.accessTokenSecret =obj2.get("access_token_secret").toString();
		
		JSONArray keywordsJson=(JSONArray)obj2.get("keywords");
		String[] kewWordsTmp = new String[keywordsJson.size()];
		for (int i = 0; i < keywordsJson.size(); i++) {
			kewWordsTmp[i] = (String) keywordsJson.get(i);
		}
		
		JSONArray fieldsJson=(JSONArray)obj2.get("emitedFields");
		String[] fieldsTmp = new String[fieldsJson.size()];
		//if (fieldsJson.size() == 0) 
		for (int i = 0; i < fieldsJson.size(); i++) {
			fieldsTmp[i] = (String) fieldsJson.get(i);
		}
		
		this.keyWords =kewWordsTmp; 
		this.fields = fieldsTmp;
		
		//new Twitter(obj2.get("consumer_key").toString(),obj2.get("consumer_secret").toString(),obj2.get("access_token").toString(),obj2.get("access_token_secret").toString(),obj2.get("keywords").toString()); 

	}
	
	public Twitter() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
			
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		
		if (keyWords.length == 0) {

			twitterStream.sample();
		}

		else {

			FilterQuery query = new FilterQuery().track(keyWords);
			twitterStream.filter(query);
		}

	}
	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			
			Object[] values = new Object[this.fields.length];
			for (int i = 0; i < this.fields.length; i++) {
				if (this.fields[i].equalsIgnoreCase("place")) {values[i] = ret.getGeoLocation();}
				else if (this.fields[i].equalsIgnoreCase("retweet_count")) {values[i] = ret.getRetweetCount();}
				else if (this.fields[i].equalsIgnoreCase("text")) {values[i] = ret.getText();}
				else if (this.fields[i].equalsIgnoreCase("hashtags")) {values[i] = ret.getHashtagEntities();}
				
			}
			
			_collector.emit(new Values(values));

		}
		
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
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
	}

}
