/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package codecs;

import java.util.HashMap;
import java.util.Map;
import state.OrderBook.OrderBookEntry;

/**
 *
 * The parser for order Strings. Has a method to convert an order string into a Map based on a Schema map given to it.
 * 
 * @author kunal
 */
public class TupleDecoder {

    Map<String, String> schema;

    /**
     * Constructor
     * 
     * @param schema The schema map
     */
    public TupleDecoder(Map<String, String> schema) {
        this.schema = schema;
    }

    /**
     * Returns a decoded map given a order string.
     * 
     * @param payload The string with the order details.
     * @return Map which contains the decoded details according to schema provided.
     */
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
            } else if (schema.get(keyValPair[0]).equals("long")) {
                o = (Object) Long.parseLong(keyValPair[1]);
            }
            else {
                System.out.println("Unidentified data type for schema");
                return null;
            }

            createdTuple.put(keyValPair[0], o);
        }

        return createdTuple;
    }
}
