package io.spring.controller;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@DependsOn({ "gemfireCache" })
public class LoaderController {

	@Autowired
	ClientCache clientCache;

	@Autowired
	Region<String, PdxInstance> transactionsRegion;

	@Autowired
	private SimpMessagingTemplate webSocket;

	InputStream inputStream = null;

	BufferedReader reader = null;

	Boolean loading_flag = false;

	Boolean isLoading = false;

	@RequestMapping(method = RequestMethod.GET, path = "/start")
	@ResponseBody
	public String start(@RequestParam(value = "batch_size", required = true) String BATCH_SIZE) throws Exception {
		Map<String, PdxInstance> buffer = new HashMap<>();

		loading_flag = true;
		isLoading = true;
		webSocket.convertAndSend("/topic/status", status());

		String attributes = "ssn|first|last|gender|street|city|state|zip|latitude|longitude|city_pop|job|dob|account_num|profile|transaction_num|transaction_date|category|amount";
		String[] keys = attributes.split("\\|");

		String line = reader.readLine();
		System.out.println(line);
		int index = count() + 1;

		while (line != null && loading_flag) {
			String[] values = line.split("\\|");
			Map<String, String> customerMap = new HashMap<>();

			if (keys.length != values.length)
				continue;

			for (int i = 0; i < keys.length; i++) {
				customerMap.put(keys[i], values[i]);
			}
			PdxInstance customer = JSONFormatter.fromJSON(new JSONObject(customerMap).toString());

			buffer.put(UUID.randomUUID().toString(), customer);

			if (index % 1000 == 0) {
				webSocket.convertAndSend("/topic/record_stats", index);
			}

			if (index % Integer.parseInt(BATCH_SIZE) == 0) {
				transactionsRegion.putAll(buffer);
				webSocket.convertAndSend("/topic/record_stats", index);
				buffer.clear();
			}

			line = reader.readLine();
			index++;

			// Thread.sleep(50);
		}

		transactionsRegion.putAll(buffer);
		isLoading = false;
		webSocket.convertAndSend("/topic/record_stats", count());
		webSocket.convertAndSend("/topic/status", status());

		return "Successfully Loaded.";
	}

	@RequestMapping(method = RequestMethod.GET, path = "/clear")
	@ResponseBody
	public String clear() throws Exception {
		transactionsRegion.removeAll(transactionsRegion.keySetOnServer());

		return "Region cleared.";
	}

	@RequestMapping(method = RequestMethod.GET, path = "/stop")
	@ResponseBody
	public String stop() throws Exception {
		loading_flag = false;

		return "Stop Signal Emitted.";
	}

	@RequestMapping(method = RequestMethod.GET, path = "/count")
	@ResponseBody
	public int count() throws Exception {
		QueryService queryService = clientCache.getQueryService();
		Query query = queryService.newQuery("SELECT count(*) FROM /Transactions");
		Object result = query.execute();
		Collection<?> collection = ((SelectResults<?>) result).asList();

		return (Integer) collection.iterator().next();
	}

	@RequestMapping(method = RequestMethod.GET, path = "/status")
	@ResponseBody
	public String status() throws Exception {
		return isLoading ? "START" : "STOP";
	}

	@PostConstruct
	public void initReader() throws Exception {
		URL FILE_LOCATION = new URL("https://s3.amazonaws.com/amey-dataset/transactions-data/trans_fact.gz");
		InputStream inputStream = new GZIPInputStream(FILE_LOCATION.openStream());
		reader = new BufferedReader(new InputStreamReader(inputStream));
	}

	@PreDestroy
	public void closeReader() throws Exception {
		if (reader != null)
			reader.close();
		if (inputStream != null)
			inputStream.close();
	}

}
