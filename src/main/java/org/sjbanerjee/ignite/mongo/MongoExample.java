package org.sjbanerjee.ignite.mongo;

import org.sjbanerjee.ignite.model.Data;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.configuration.FactoryBuilder;

import static org.sjbanerjee.ignite.utils.DataOps.*;

public class MongoExample {
    private static final int DATA_LIMIT = 100;

    public static void main(String[] args){
        try(Ignite ignite = Ignition.start("/Users/Sjbanerjee/apache-ignite-fabric-2.2.0-bin/examples/config/example-ignite.xml")){
            test(ignite, false);
        }
    }

    public static void test (Ignite ignite, boolean writeBehind){
        CacheConfiguration<Integer, Data> cc = new CacheConfiguration<>("Data");
        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheMongoStore.class));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);

        // Set write-behind flag for synchronous/asynchronous persistent store update
        cc.setWriteBehindEnabled(writeBehind);
        IgniteCache<Integer, Data> cache = ignite.getOrCreateCache(cc);
        
        if(cache.size() <= 0)
            cache.localLoadCache(null);//??

        //Initialize data population
        initializeData(cache, DATA_LIMIT);

        //Query data
        queryData_scanQuery(cache);


         /*************************************************************************************************************************
         * This part onwards is for testing whether the data is fetched from cache or mongodb once it is loaded to ignite memory *
         *************************************************************************************************************************/

        //update a data in the mongodb
        Data data = new Data("new Data", "new Data", 99, "new subdata99");
        cache.put(99, data);

        //Query data
        queryData_scanQuery(cache);
    }
}
