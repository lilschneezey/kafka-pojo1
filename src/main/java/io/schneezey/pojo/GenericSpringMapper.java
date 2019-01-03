package io.schneezey.pojo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.springframework.beans.PropertyAccessorFactory;

/*
 * Notes:
 * 		Supports: int, long, double, float, boolean, Integer, Long, Double, Float, String
 * 		Supports: null values
 * 		Does not support: bytes[], enum, java.util.Date
 */
public class GenericSpringMapper<T> {
	
    public GenericRecord mapObjectToRecord(T pojo) {
        final Schema schema = ReflectData.get().getSchema(pojo.getClass());
        final GenericData.Record record = new GenericData.Record(schema);
        schema.getFields().forEach(r -> 
        	record.put(r.name(), PropertyAccessorFactory.forDirectFieldAccess(pojo).getPropertyValue(r.name()))
        );
        return record;
    }

    public T mapRecordToObject(GenericRecord record, T pojo) {

        record.getSchema().getFields().forEach(d -> 
        	PropertyAccessorFactory.forDirectFieldAccess(pojo)
        	.setPropertyValue(d.name(), record.get(d.name()) == null ? record.get(d.name()) : record.get(d.name()).toString())
        );
        return pojo;
    }
    
}
