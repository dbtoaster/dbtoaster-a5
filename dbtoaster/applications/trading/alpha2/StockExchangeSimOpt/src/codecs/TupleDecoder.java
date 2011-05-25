/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package codecs;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author kunal
 */
public class TupleDecoder {

    Map<String, String> schema;

    public TupleDecoder(Map<String, String> schema) {
        this.schema = schema;
    }

    public synchronized Map<String, Object> createTuples(String payload) {
        payload = payload.trim();
        String[] fields = payload.split(" ");
        Map<String, Object> createdTuple = new HashMap<String, Object>();

        for (String item : fields) {
            String[] keyValPair = item.split(":");

            if (keyValPair.length != 2) {
                System.out.println("Input tuple is not a valid key-value pair");
                return null;
            }

            Object o;
            if (schema.get(keyValPair[0]) == null) {
                //Unidentified information in the string. Ignore
                continue;
            } else if (schema.get(keyValPair[0]).equals("int")) {
                o = (Object) Integer.parseInt(keyValPair[1]);

            } else if (schema.get(keyValPair[0]).equals("string")) {
                o = (Object) keyValPair[1];
            } else if (schema.get(keyValPair[0]).equals("double")) {
                o = (Object) Double.parseDouble(keyValPair[1]);
            } else {
                System.out.println("Unidentified data type for schema");
                return null;
            }

            createdTuple.put(keyValPair[0], o);
        }

        return createdTuple;
    }
}
