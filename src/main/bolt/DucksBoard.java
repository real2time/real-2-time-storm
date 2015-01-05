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

import java.util.*;

import com.bring.ducksboard.DucksboardDao;
import com.bring.ducksboard.DucksboardId;
import com.bring.ducksboard.DucksboardService;

import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class DucksBoard extends BaseRichBolt {

	int count = 0;
	DucksboardDao dao;// = new DucksboardDao(new DefaultHttpClient(), "XEmNNBQb1M8142xtoi3C20NON2VkcPLuo1FdA1HKZXQGcmh1Nn");
    DucksboardService service;// = new DucksboardService(dao);
    DucksboardId id;// = service.createId("522245");
    String idString;
    String keyString;
    
    public DucksBoard(String json){
    	Object obj=JSONValue.parse(json);
		JSONArray array=(JSONArray)obj;
		JSONObject obj2=(JSONObject)array.get(0);
		this.idString = obj2.get("id").toString();
		this.keyString = obj2.get("key").toString();
		
    }
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    
		dao = new DucksboardDao(new DefaultHttpClient(), keyString);
	    service = new DucksboardService(dao);
	    id = service.createId(idString);
	}

  @Override
  public void execute(Tuple tuple) {
    // System.out.println(tuple);
    count++;
    System.out.println(this.idString+" "+count);
	service.pushValue(id, count);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}