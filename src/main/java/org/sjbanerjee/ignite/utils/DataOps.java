package org.sjbanerjee.ignite.utils;

import org.sjbanerjee.ignite.model.Data;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.lang.IgniteClosure;

public class DataOps {

    public static void queryData_SQLQuery(IgniteCache cache) {
        SqlQuery sql = new SqlQuery<AffinityKey<Integer>, Data>(Data.class, "index > ?");

        // Find all data with index greater than 90.
        System.out.println("Query result SQL query: ");
        cache.query(sql.setArgs(90)).getAll()
                .forEach(x -> System.out.println(x.toString()));
    }

    public static void queryData_scanQuery(IgniteCache cache) {
        //Filter
        ScanQuery query = new ScanQuery();
        query.setFilter((key, data) -> {
            Data d = (Data) data;
            return d.getIndex() > 90;
        });

        // Find all data with index greater than 90.
        System.out.println("Query result using Scan Query: ");
        cache.query(query).getAll()
                .forEach(x -> System.out.println(x.toString()));
    }

    public static void queryData_scanQuery_SpecificFields(IgniteCache cache) {
        //Filter
        ScanQuery query = new ScanQuery();
        query.setFilter((key, data) -> {
            Data d = (Data) data;
            return d.getIndex() > 90;
        });

        // Transformer
        IgniteClosure transformer = new IgniteClosure() {
            @Override
            public Object apply(Object e) {
                return null;
            }
        };

        // Find all data with index greater than 90.
        System.out.println("Query result : ");
        cache.query(query, transformer).getAll()
                .forEach(x -> System.out.println(x));
    }

    public static void queryData_SQLQueryStmt(IgniteCache cache, String queryString){
        SqlFieldsQuery sql = new SqlFieldsQuery(queryString);

        System.out.println("Query result SQL query: ");
        cache.query(sql).getAll()
                .forEach(x -> System.out.println(x.toString()));
    }

    /**
     * Need to set the field in the POJO as @QueryTextField
     *
     * @param cache
     */
    public static void queryData_TextQuery(IgniteCache cache, String fieldVal) {
        TextQuery query = new TextQuery(Data.class, fieldVal);

        // Find all data with index greater than 90.
        System.out.println("Query result using text query: ");
        cache.query(query).getAll()
                .forEach(x -> System.out.println(x.toString()));
    }

    public static void initializeData(IgniteCache cache, int size) {
        System.out.println("Populating " + size + " data entries.");
        //Preparing data
        for (int i = 0; i < size; i++) {
            Data data = new Data("id" + i, "name" + i, i, "subdata" + i);
            System.out.println("Putting data : \n" + data.toString());
            cache.put(i, data);
        }
    }
}
