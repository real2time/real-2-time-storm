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
package bolt;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import java.sql.Timestamp;
import java.util.*;

import com.bring.ducksboard.DucksboardDao;
import com.bring.ducksboard.DucksboardId;
import com.bring.ducksboard.DucksboardRequest;
import com.bring.ducksboard.DucksboardService;
import com.google.gson.JsonObject;

import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import twitter4j.GeoLocation;
import util.DucksboardDao2;

public class DucksBoardMap extends BaseRichBolt {

	int count = 0;
	DucksboardDao2 dao;// = new DucksboardDao(new DefaultHttpClient(), "XEmNNBQb1M8142xtoi3C20NON2VkcPLuo1FdA1HKZXQGcmh1Nn");
    DucksboardService service;// = new DucksboardService(dao);
    DucksboardId id;// = service.createId("522245");
    String idString;
    String keyString;
    String[] fields;
    
    public DucksBoardMap(String id, String key){
    	this.idString = id;
    	this.keyString = key;
    }
    
    public DucksBoardMap(String json){
    	Object obj=JSONValue.parse(json);
		JSONArray array=(JSONArray)obj;
		JSONObject obj2=(JSONObject)array.get(0);
		this.idString = obj2.get("id").toString();
		this.keyString = obj2.get("key").toString();
		JSONArray fieldsJson=(JSONArray)obj2.get("requiredFields");
		String[] fieldsTmp = new String[fieldsJson.size()];
		//if (fieldsJson.size() == 0) 
		for (int i = 0; i < fieldsJson.size(); i++) {
			fieldsTmp[i] = (String) fieldsJson.get(i);
		}
		
		this.fields = fieldsTmp;
		
    }
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    
		dao = new DucksboardDao2(new DefaultHttpClient(), keyString);
	   
	}

  @Override
  public void execute(Tuple tuple) {
	  GeoLocation tmp = (GeoLocation) tuple.getValueByField(this.fields[0]);
	  
	  if (tmp != null){
		  
		  String val = "{\"value\": {\"latitude\": "+tmp.getLatitude()+",\"longitude\": "+tmp.getLongitude()+" }}";
		  dao.push(idString,val); 
	  }

	
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}