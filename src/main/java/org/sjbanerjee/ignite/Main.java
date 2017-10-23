package org.sjbanerjee.ignite;

import org.sjbanerjee.ignite.model.Data;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.sjbanerjee.ignite.utils.DataOps;

public class Main {
    private static final int DATA_SIZE = 100;

    public static void main(String[] args) throws IgniteException {

//        try(Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Native Persistence configuration.
        PersistentStoreConfiguration psCfg = new PersistentStoreConfiguration();
        psCfg.setPersistentStorePath("/Users/jf45/store");
        System.out.println("Persistent Store path : " + psCfg);

        // Enabling the Persistent Store.
        cfg.setPersistentStoreConfiguration(psCfg);

        //Work directory for write-ahead log
        cfg.setWorkDirectory("/Users/SjBanerjee/workspace");
        System.out.println("Default work directory location : " + cfg.getWorkDirectory());

        Ignite ignite = Ignition.start(cfg);

        // Activating the cluster once all the cluster nodes are up and running.
        ignite.active(true);

        CacheConfiguration<Integer, Data> cacheCfg = new CacheConfiguration<>("Data");
        cacheCfg.setIndexedTypes(Integer.class, Data.class);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        IgniteCache<Integer, Data> cache = ignite.getOrCreateCache(cacheCfg);

        // Populate data
        DataOps.initializeData(cache, DATA_SIZE);

        // Query data using SQL
//        queryData_SQLQuery(cache);

        // Query data using Scan Query
//        queryData_scanQuery(cache);

        // Query data using Scan Query and retrieve specific fields
//      queryData_scanQuery_SpecificFields(cache);

        // Query data using SQL Query
//      queryData_SQLQueryStmt(cache, "SELECT * from Data where (subData REGEXP 'subdata5')");

        // Query data using Text Query
        DataOps.queryData_TextQuery(cache, "subdata5");

        //Finally close ignite
        ignite.close();
    }

}
