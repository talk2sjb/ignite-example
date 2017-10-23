package org.sjbanerjee.ignite.mongo;

import com.mongodb.*;
import org.sjbanerjee.ignite.model.Data;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.*;
import de.flapdoodle.embed.process.runtime.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.lifecycle.*;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import javax.cache.*;
import javax.cache.integration.*;
import java.io.*;
import java.util.*;


/**
 * Sample MongoDB embedded cache store.
 *
 */
public class CacheMongoStore extends CacheStoreAdapter<Integer, Data> implements LifecycleAware{

    /** MongoDB port. */
    private static final int MONGOD_PORT = 27001;

    /** MongoDB executable for embedded MongoDB store. */
    private MongodExecutable mongoExe;

    /** Mongo data store. */
    private Datastore morphia;

    /** {@inheritDoc} */
    @Override
    public void start() throws IgniteException {
        MongodStarter starter = MongodStarter.getDefaultInstance();

        try {
            IMongodConfig mongoCfg = new MongodConfigBuilder().
                    version(Version.Main.PRODUCTION).
                    net(new Net(MONGOD_PORT, Network.localhostIsIPv6())).
                    build();

            mongoExe = starter.prepare(mongoCfg);

            mongoExe.start();

            System.out.println("Embedded MongoDB started.");

            MongoClient mongo = new MongoClient("localhost", MONGOD_PORT);

            Set<Class> clss = new HashSet<>();

            Collections.addAll(clss, Data.class);

            morphia = new Morphia(clss).createDatastore(mongo, "Data");
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws IgniteException {
        if (mongoExe != null) {
            mongoExe.stop();

            System.out.println("Embedded mongodb stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override
    public Data load(Integer key) throws CacheLoaderException {
        Data data = morphia.find(Data.class).field("id").equal(key).get();
        System.out.println("Loaded data: " + data);

        return data;
    }

    /** {@inheritDoc} */
    @Override
    public void write(Cache.Entry<? extends Integer, ? extends Data> e) throws CacheWriterException {
        morphia.save(e.getValue());

        System.out.println("Stored data: " + e.getValue());
    }

    /** {@inheritDoc} */
    @Override
    public void delete(Object key) throws CacheWriterException {
        Data data = morphia.find(Data.class).field("id").equal(key).get();

        if (data != null) {
            morphia.delete(data);

            System.out.println("Removed data: " + key);
        }
    }
}

