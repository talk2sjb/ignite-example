package org.sjbanerjee.ignite.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.mongodb.morphia.annotations.Id;

import java.io.Serializable;

public class Data implements Serializable{

    @Id
    String id;

    @QuerySqlField(index = true)
    String name;

    @QuerySqlField(index = true)
    int index;

    @QueryTextField
    String subData; //For textQuery

    public Data(){
        //Nothing
    }

    public Data(String id, String name, int index){
        this.id = id;
        this.name = name;
        this.index = index;
    }

    public Data(String id, String name, int index, String subData){
        this.id = id;
        this.name = name;
        this.index = index;
        this.subData  = subData;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public String getSubData() {
        return subData;
    }

    @Override
    public String toString() {
        return "{" +
                "id: \"" + id + '\"' +
                ", name: \"" + name + '\"' +
                ", index: \"" + index + '\"' +
                ", subData: \"" + subData + '\"' +
                '}';
    }
}
