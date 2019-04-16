package io.spring.controller;

import java.io.BufferedReader;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.company.Company;
import io.codearte.jfairy.producer.payment.CreditCard;
import io.codearte.jfairy.producer.person.Person;

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
	
	Fairy fairy;

	@RequestMapping(method = RequestMethod.GET, path = "/start")
	@ResponseBody
	public String start(@RequestParam(value = "batch_size", required = true) String BATCH_SIZE) throws Exception {
		Map<String, PdxInstance> buffer = new HashMap<>();

		loading_flag = true;
		isLoading = true;
		webSocket.convertAndSend("/topic/status", status());

		int index = count() + 1;

		while (loading_flag) {
			Map<String, String> customerMap = new HashMap<>();

			readAdditionalColumns(customerMap);
				
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
//		URL FILE_LOCATION = new URL("https://s3.amazonaws.com/amey-dataset/transactions-data/trans_fact.gz");
//		InputStream inputStream = new GZIPInputStream(FILE_LOCATION.openStream());
//		reader = new BufferedReader(new InputStreamReader(inputStream));
		fairy = Fairy.create();
	}

	@PreDestroy
	public void closeReader() throws Exception {
		if (reader != null)
			reader.close();
		if (inputStream != null)
			inputStream.close();
	}
	
	private void readAdditionalColumns(Map<String, String> customerMap) throws Exception {
		

		 Company sampleCompany = fairy.company();
		 Person samplePerson = fairy.person();
		 customerMap.put("company_name", sampleCompany.name());
		 customerMap.put("company_domain", sampleCompany.domain());
		 customerMap.put("company_email", sampleCompany.email());
		 customerMap.put("company_url", sampleCompany.url());
		 customerMap.put("company_street", samplePerson.getAddress().street());
		 customerMap.put("company_street_no", samplePerson.getAddress().streetNumber());
		 customerMap.put("company_city", samplePerson.getAddress().getCity());
		 customerMap.put("company_postalcode", samplePerson.getAddress().getPostalCode());
		 customerMap.put("company_building_no", samplePerson.getAddress().apartmentNumber());
		 
		 CreditCard creditCard =  fairy.creditCard();
		 customerMap.put("cc_vendor", creditCard.vendor());
		 customerMap.put("cc_expirydate_str", creditCard.expiryDateAsString());
		 customerMap.put("cc_expirydate", creditCard.expiryDate().toString());
		 customerMap.put("cc_street", samplePerson.getAddress().street());
		 customerMap.put("cc_street_no", samplePerson.getAddress().streetNumber());
		 customerMap.put("cc_city", samplePerson.getAddress().getCity());
		 customerMap.put("cc_postalcode", samplePerson.getAddress().getPostalCode());
		 customerMap.put("cc_building_no", samplePerson.getAddress().apartmentNumber());
		 
		 Person secondFamilyMember = fairy.person();
		 
		 customerMap.put("person_2_companyEmail", secondFamilyMember.companyEmail());
		 customerMap.put("person_2_personalEmail", secondFamilyMember.email());
		 customerMap.put("person_2_firstName", secondFamilyMember.firstName());
		 customerMap.put("person_2_lastName", secondFamilyMember.lastName());
		 customerMap.put("person_2_fullName", secondFamilyMember.fullName());
		 customerMap.put("person_2_middleName", secondFamilyMember.middleName());
		 customerMap.put("person_2_id", secondFamilyMember.nationalIdentificationNumber());
		 customerMap.put("person_2_passportno", secondFamilyMember.passportNumber());
		 customerMap.put("person_2_mobile", secondFamilyMember.telephoneNumber());
		 customerMap.put("person_2_dob", secondFamilyMember.dateOfBirth().toString());
		 customerMap.put("person_2_sex", secondFamilyMember.sex().toString());
		 customerMap.put("person_2_password", secondFamilyMember.password());
		 customerMap.put("person_2_street", secondFamilyMember.getAddress().street());
		 customerMap.put("person_2_street_no", secondFamilyMember.getAddress().streetNumber());
		 customerMap.put("person_2_city", secondFamilyMember.getAddress().getCity());
		 customerMap.put("person_2_postalcode", secondFamilyMember.getAddress().getPostalCode());
		 customerMap.put("person_2_building_no", secondFamilyMember.getAddress().apartmentNumber());
		 
		 
		 CreditCard creditCard2 =  fairy.creditCard();
		 customerMap.put("cc2_vendor", creditCard2.vendor());
		 customerMap.put("cc2_expirydate_str", creditCard2.expiryDateAsString());
		 customerMap.put("cc2_expirydate", creditCard2.expiryDate().toString());
		 customerMap.put("cc2_street", secondFamilyMember.getAddress().street());
		 customerMap.put("cc2_street_no", secondFamilyMember.getAddress().streetNumber());
		 customerMap.put("cc2_city", secondFamilyMember.getAddress().getCity());
		 customerMap.put("cc2_postalcode", secondFamilyMember.getAddress().getPostalCode());
		 customerMap.put("cc2_building_no", secondFamilyMember.getAddress().apartmentNumber());
		
	}

}
