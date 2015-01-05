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

import java.math.BigDecimal;
import java.util.*;

import com.bring.ducksboard.DucksboardDao;
import com.bring.ducksboard.DucksboardId;
import com.bring.ducksboard.DucksboardService;

import org.apache.http.impl.client.DefaultHttpClient;

public class DucksBoardBolt extends BaseRichBolt {

	DucksboardDao dao;// = new DucksboardDao(new DefaultHttpClient(), "XEmNNBQb1M8142xtoi3C20NON2VkcPLuo1FdA1HKZXQGcmh1Nn");
    DucksboardService service;// = new DucksboardService(dao);
    DucksboardId id;// = service.createId("522245");
    DucksboardId id2, id3;

    int totalCount = 0;
    HashMap<String, Integer> counts;
    
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    
		dao = new DucksboardDao(new DefaultHttpClient(), "XEmNNBQb1M8142xtoi3C20NON2VkcPLuo1FdA1HKZXQGcmh1Nn");
	    service = new DucksboardService(dao);
	    id = service.createId("524587"); //Madrid
	    id2 = service.createId("524588"); //Barca
	    id3 = service.createId("522245"); //total
	
	    counts = new HashMap<String, Integer>();
	    counts.put("spout-madrid", 0);
	    counts.put("spout-barca", 0);
    
	}

  @Override
  public void execute(Tuple tuple) {
//	  System.out.println("INFO: " + tuple.getSourceComponent() + " - " + tuple.getSourceTask());
    // System.out.println(tuple);
	  String from = tuple.getSourceComponent();
	  int val;
	  
	  if (counts.containsKey(from)) {
		  val = counts.get(from);
	  } else {
		  val = 0;
	  }
	  
	  val++;

	  counts.put(from, val);
	  
	  totalCount++;

//	  for (String key : counts.keySet()) {
//		  service.pushValue(id, counts.get(key) / totalCount);
//	  }
	  
	  if ((totalCount % 10) == 0) {
		  double value = (double)counts.get("spout-madrid") / (double)totalCount;
		  double value2 = (double)counts.get("spout-barca") / (double)totalCount;
	//	  System.out.println("MADRID: " + value);
	//	  System.out.println("BARCA: " + value2);
		  service.pushValue(id, BigDecimal.valueOf(value));
		  service.pushValue(id2, BigDecimal.valueOf(value2));
		  service.pushValue(id3, totalCount);
	  }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
