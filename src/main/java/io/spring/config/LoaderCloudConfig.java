package io.spring.config;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.PdxInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.gemfire.config.annotation.EnableLogging;
import org.springframework.geode.config.annotation.UseMemberName;


@EnableLogging(logLevel = "config")
@UseMemberName(value = "DataLoaderClient")
@Configuration
public class LoaderCloudConfig {


	@Bean(name = "transactionsRegion")
	@DependsOn({"gemfireCache"})
	public Region<String, PdxInstance> transactionRegion(@Autowired ClientCache clientCache) {
		ClientRegionFactory<String, PdxInstance> transactionRegionFactory = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
		Region<String, PdxInstance> transactionRegion = transactionRegionFactory.create("Transactions");
		return transactionRegion;
	}

}
