package org.dbtoaster.gui;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

public class DatastructureViewer
{
    final static int DEFAULT_NUMERIC_BUCKET_WIDTH = 10000;
    final static int DEFAULT_STRING_BUCKET_PREFIX_LENGTH = 2;
    
    interface ViewerNode
    {
        // Content provider interface
        public Object[] getChildrenCP();
        public Object getParentCP();
        public boolean hasChildrenCP();
        
        // Label provider interface
        public String getTextLP();

        // Tree interface
        public boolean isLeaf();

        public int getChildCount();
        public int getLeafCount();

        public void addTuple(Object tuple)
            throws TreeException, DuplicateException;

        public void addKeyValue(Object key, Object value)
            throws TreeException, DuplicateException;
    }

    enum DatastructureType { LIST, MAP, SET };

    class DatastructureTypeException extends Exception
    {
        private static final long serialVersionUID = 3352397739969240996L;
        public DatastructureTypeException(String msg) { super(msg); }
    };
    
    class IntervalException extends Exception
    {
        private static final long serialVersionUID = 9207578858129361168L;
        public IntervalException(String msg) { super(msg); }
    }

    class TreeException extends Exception
    {
        private static final long serialVersionUID = 1983820727566988271L;
        public TreeException(String msg) { super(msg); }
    };

    class DuplicateException extends Exception
    {
        private static final long serialVersionUID = 2965550785002477844L;
        
        public DuplicateException(String msg) { super(msg); }
    };

    class Interval<T extends Comparable<? super T>> 
    {
        T lower;
        T upper;

        Interval(T l, T u) throws IntervalException
        {
            if ( l.compareTo(u) == 0 ) {
                throw new IntervalException(
                    "Invalid interval " + l.toString() + ", " + u.toString());
            }
            lower = l; upper = u;
        }
        
        public T getLower () { return lower; }

        public T getUpper () { return upper; }

        public boolean contains(Object point)
        {
            T p = (T) point;
            return (lower.compareTo(p) <= 0) && (0 <= upper.compareTo(p));
        }

        public String toString() {
            return "[" + lower.toString() + "," + upper.toString() + "]";
        }
    }

    interface BucketStrategy<T extends Comparable<? super T>>
    {
        Interval<T> getBucket(Object key) throws IntervalException;
    }
    
    class FixedIntegerBucket implements BucketStrategy<Integer>
    {
        Integer span;
        FixedIntegerBucket(Integer span) {
            this.span = span;
        }

        public Interval<Integer> getBucket(Object key)
            throws IntervalException
        {
            Interval<Integer> r = null;
            Integer lower = ((Integer) key) / span;
            Integer upper = lower + 1;
            r = new Interval<Integer>(lower, upper);
            return r;
        }
    }
    
    class FixedLongBucket implements BucketStrategy<Long>
    {
        Long span;
        FixedLongBucket(Long span) {
            this.span = span;
        }

        public Interval<Long> getBucket(Object key)
            throws IntervalException
        {
            Interval<Long> r = null;
            Long lower = ((Long) key) / span;
            Long upper = lower + 1;
            r = new Interval<Long>(lower, upper);
            return r;
        }
    }

    class FixedFloatBucket implements BucketStrategy<Float>
    {
        Float span;
        FixedFloatBucket(Float span) {
            this.span = span;
        }

        public Interval<Float> getBucket(Object key)
            throws IntervalException
        {
            Interval<Float> r = null;
            Float lower = ((Float) key) / span;
            Float upper = lower + 1;
            r = new Interval<Float>(lower, upper);
            return r;
        }
    }
    
    class FixedDoubleBucket implements BucketStrategy<Double>
    {
        Double span;
        FixedDoubleBucket(Double span) {
            this.span = span;
        }

        public Interval<Double> getBucket(Object key)
            throws IntervalException
        {
            Interval<Double> r = null;
            Double lower = ((Double) key) / span;
            Double upper = lower + 1;
            r = new Interval<Double>(lower, upper);
            return r;
        }
    }
    
    class FixedStringBucket implements BucketStrategy<String>
    {
        int prefixLength;
        FixedStringBucket(int prefixLength) {
            this.prefixLength = prefixLength;
        }
        
        public Interval<String> getBucket(Object key) throws IntervalException
        {
            String k = (String) key;
            String lower = k.substring(0, prefixLength) + ((char) 0);
            String upper = k.substring(0, prefixLength) + ((char) 255);
            return new Interval<String>(lower, upper);
        }
    }

    class DataStructureNode implements ViewerNode
    {
        String dsName;
        DatastructureType dsType;
        Class<?> dsClassType;

        TreeNode dsRoot;

        // Content provider methods
        public Object[] getChildrenCP()
        {
            return dsRoot.getChildrenCP();
        }

        public Object getParentCP()
        {
            return null;
        }

        public boolean hasChildrenCP()
        {
            return dsRoot.hasChildrenCP();
        }

        // Label provider methods.
        public String getTextLP() { return dsName; }

        // Tree methods.
        public DataStructureNode(String name, DatastructureType type, Class<?> cl)
        {
            dsName = name;
            dsType = type;
            dsClassType = cl;

            Interval<?> initBucket = null;
            LinkedList<Field> dimensions = new LinkedList<Field>();
            LinkedList<BucketStrategy<?>> dimensionBuckets =
                new LinkedList<BucketStrategy<?>>();
            
            try
            { 
                for (Field f : cl.getFields())
                {
                    dimensions.add(f);
                    dimensionBuckets.add(getBucketStrategy(f.getType()));
                    
                    if ( initBucket == null ) {
                        Object defaultFieldVal = f.getType().newInstance();
                        initBucket = dimensionBuckets.getLast().getBucket(defaultFieldVal);
                    }
                }

                dsRoot = new TreeNode(dsType == DatastructureType.LIST,
                    initBucket, dimensions, dimensionBuckets);

            } catch (Exception e) {
                System.out.println("Failed to create root for " + name);
                e.printStackTrace();
            }
        }
        
        BucketStrategy<?> getBucketStrategy(Class<?> fieldType)
            throws DatastructureTypeException
        {
            BucketStrategy<?> r = null;

            if ( fieldType.equals(Void.TYPE) || fieldType.equals(Boolean.TYPE)
                    || fieldType.equals(Character.TYPE)
                    || fieldType.equals(Byte.TYPE)
                    || fieldType.equals(Short.TYPE) )
            {
                String msg = "Unable to support field type " + fieldType.getName();
                System.err.println(msg);
                throw new DatastructureTypeException(msg);
            }
            else if ( fieldType.equals(Integer.TYPE) ) {
                r = new FixedIntegerBucket(DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else if ( fieldType.equals(Long.TYPE) ) {
                r = new FixedLongBucket((long) DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else if ( fieldType.equals(Float.TYPE) ) {
                r = new FixedFloatBucket((float) DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else if ( fieldType.equals(Double.TYPE) ) {
                r = new FixedDoubleBucket((double) DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else
                System.err.println("Invalid primitive type " + fieldType.getName());
            
            return r;
        }

        public boolean isLeaf() { return dsRoot.isLeaf(); }

        public int getChildCount() { return dsRoot.getChildCount(); }
        
        public int getLeafCount() { return dsRoot.getLeafCount(); }
        
        public void addTuple(Object tuple)
            throws TreeException, DuplicateException
        {
            dsRoot.addTuple(tuple);
        }

        public void addTupleToDatastructure(Object tuple)
            throws DatastructureTypeException
        {
            if ( dsType.equals(DatastructureType.MAP) ) {
                String msg = "Cannot add tuple " + tuple + " to map " + dsName;
                throw new DatastructureTypeException(msg);
            }

            try {
                addTuple(tuple);
            } catch (Exception e) {
                System.err.println("Could not add tuple: " + tuple);
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }
        
        public void addKeyValue(Object key, Object value)
            throws TreeException, DuplicateException
        {
            dsRoot.addKeyValue(key, value);
        }
        
        public void addKeyValueToDatastructure(Object key, Object value)
            throws DatastructureTypeException
        {
            if ( dsType.equals(DatastructureType.LIST) ||
                    dsType.equals(DatastructureType.SET) )
            {
                String msg = "Cannot add key/value <" +
                    key + ", " + value + "> to domain " + dsName;
                throw new DatastructureTypeException(msg);
            }
            
            try {
                addKeyValue(key, value);
            } catch (Exception e) {
                System.err.println("Could not add key/pair: <" + key + ", " + value + ">");
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }
        
    }

    class TreeNode implements ViewerNode
    {
        boolean allowDuplicates;

        ViewerNode parent;
        LinkedList<Interval<?>> parentValues;
        
        // Bucketing should work on child dimension field.
        Interval<?> bucket;
        BucketStrategy<?> bucketStrategy;

        // Children
        Field childDimension;
        Class<?> childDimensionType;
        TreeMap<Object, ViewerNode> children;

        // Subtree bucket strategies should work on field types of
        // subtree dimensions
        LinkedList<Field> subTreeDimensions;
        LinkedList<BucketStrategy<?>> subTreeBucketing;

        // Content provider methods.
        
        public Object[] getChildrenCP()
        {
            return children.values().toArray();
        }

        public Object getParentCP()
        {
            return parent;
        }

        public boolean hasChildrenCP()
        {
            return !children.isEmpty();
        }

        // Label provider methods.
        public String getTextLP()
        {
            String r = "";
            for (Interval<?> i : parentValues) r += i.toString();
            return r;
        }

        // Tree methods.

        // Root node constructor
        public TreeNode(boolean duplicates, Interval<?> bucket,
            LinkedList<Field> stDimensions, LinkedList<BucketStrategy<?>> bs)
            throws TreeException, DuplicateException
        {
            init(duplicates, null, null, bucket, stDimensions, bs);
        }

        // Incremental constructors
        public TreeNode(boolean duplicates, ViewerNode par,
                LinkedList<Interval<?>> parValues, Interval<?> bucket,
                LinkedList<Field> stDimensions,
                LinkedList<BucketStrategy<?>> bs, Object tuple)
            throws TreeException, DuplicateException
        {
            init(duplicates, par, parValues, bucket, stDimensions, bs);
            addTuple(tuple);
        }

        public TreeNode(boolean duplicates, ViewerNode par,
                LinkedList<Interval<?>> parValues, Interval<?> bucket,
                LinkedList<Field> stDimensions,
                LinkedList<BucketStrategy<?>> bs, Object key, Object value)
            throws TreeException, DuplicateException
        {
            init(duplicates, par, parValues, bucket, stDimensions, bs);
            addKeyValue(key, value);
        }

        void init(boolean duplicates, ViewerNode par, LinkedList<Interval<?>> parValues,
                Interval<?> bucket, LinkedList<Field> stDimensions,
                LinkedList<BucketStrategy<?>> strategies)
            throws TreeException
        {
            allowDuplicates = duplicates;
            parent = par;
            parentValues = parValues;
            this.bucket = bucket;

            children = new TreeMap<Object, ViewerNode>();
            
            if ( stDimensions.size() != strategies.size() ) {
                String msg = "Inconsistent dimensions/bucketing: " +
                    stDimensions.size() + ", " + strategies.size();
                throw new TreeException(msg);
            }

            if ( stDimensions.size() > 1 ) {
                subTreeDimensions = new LinkedList<Field>();
            
                subTreeDimensions.addAll(stDimensions);
                childDimension = subTreeDimensions.pop();
                childDimensionType = childDimension.getType();
                
                subTreeBucketing = new LinkedList<BucketStrategy<?>>();
                subTreeBucketing.addAll(strategies);
                bucketStrategy = subTreeBucketing.pop();
            }
            else if ( stDimensions.size() == 1 ) {
                subTreeDimensions = null;
                childDimension = stDimensions.getFirst();
                childDimensionType = childDimension.getType();
                
                subTreeBucketing = null;
                bucketStrategy = strategies.getFirst();
            }
            else {
                String msg = "Found zero dimension tree node.";
                throw new TreeException(msg);
            }
        }
        
        void enforceContainment(Object childKey) throws TreeException
        {
            if ( !bucket.contains(childKey) ) {
                String msg = "Child lies outside node, bucket: " +
                    bucket.toString() + " val: " + childKey.toString();
                throw new TreeException(msg);
            }
        }

        LinkedList<Interval<?>> getNodeBucket()
        {
            LinkedList<Interval<?>> r = new LinkedList<Interval<?>>();
            if ( parentValues != null ) r.addAll(parentValues);
            if ( bucket != null ) r.add(bucket);
            return r;
        }

        public boolean isLeaf() { return false; }

        public int getChildCount() { return children.size(); }
        
        public int getLeafCount()
        { 
            int r = 0;
            for (Map.Entry<Object, ViewerNode> e : children.entrySet())
                r += e.getValue().getLeafCount();
            return r;
        }

        public void addTuple(Object tuple)
            throws TreeException, DuplicateException
        {
            Object childDimVal;
            try {
                childDimVal = childDimension.get(tuple);
            } catch (Exception e) {
                String msg = "Failed to get child field " +
                    childDimension.getName() + " from " +
                    tuple.getClass().getName();

                e.printStackTrace();
                throw new TreeException(msg);
            }

            enforceContainment(childDimVal);

            Object childKey = children.ceilingKey(childDimVal);
            if ( childKey == null )
            {
                // Add new subtree
                if ( subTreeDimensions == null ) {
                    children.put(childKey,
                        new MapElementNode(allowDuplicates, tuple));
                }
                
                else
                {
                    try {
                        Interval<?> newChildBucket =
                            bucketStrategy.getBucket(childKey);
                        
                        TreeNode newSubTree = new TreeNode(
                            allowDuplicates, this, getNodeBucket(), newChildBucket,
                            subTreeDimensions, subTreeBucketing, tuple);

                        children.put(newChildBucket.getUpper(), newSubTree);

                    } catch (IntervalException e) {
                        e.printStackTrace();
                    }
                }
            }
            else {
                ViewerNode childNode = children.get(childKey);
                childNode.addTuple(tuple);
            }
        }

        public void addKeyValue(Object key, Object value)
            throws TreeException, DuplicateException
        {
            Object childDimVal;
            try {
                childDimVal = childDimension.get(key);
            } catch (Exception e) {
                String msg = "Failed to get child field " +
                    childDimension.getName() + " from " +
                    key.getClass().getName();

                e.printStackTrace();
                throw new TreeException(msg);
            }

            enforceContainment(childDimVal);

            Object childKey = children.ceilingKey(childDimVal);
            if ( childKey == null )
            {
                // Add new subtree
                if ( subTreeDimensions == null )
                    children.put(childKey, new MapElementNode(key, value));

                else {
                    try {
                        Interval<?> newChildBucket = bucketStrategy.getBucket(childKey);
    
                        TreeNode newSubTree = new TreeNode(
                            allowDuplicates, this, getNodeBucket(), newChildBucket,
                            subTreeDimensions, subTreeBucketing, key, value);
    
                        children.put(newChildBucket.getUpper(), newSubTree);
                    } catch (IntervalException e) {
                        e.printStackTrace();
                    }
                }
            }
            else {
                ViewerNode childNode = children.get(childKey);
                childNode.addKeyValue(key, value);
            }
        }
        
    }
    
    class MapElementNode implements ViewerNode
    {
        ViewerNode parent;

        Object key;         // null for non-map types.
        Object value;
        
        boolean allowDuplicates;
        int count;          // for duplicates
        
        // Content provider methods.

        public Object[] getChildrenCP()
        {
            return null;
        }

        public Object getParentCP()
        {
            return parent;
        }

        public boolean hasChildrenCP()
        {
            return false;
        }

        // Label provider methods
        public String getTextLP() {
            String r = "";
            if ( key != null ) { r += key.toString() + ": "; }
            r += value.toString();
            return r;
        }

        // Tree methods
        public MapElementNode(boolean duplicates, Object value)
        {
            this.value = value;
            count = 1;
            allowDuplicates = duplicates;
        }

        public MapElementNode(Object key, Object value) {
            this.key = key; this.value = value; count = 1;
            allowDuplicates = false;
        }
        
        public boolean isLeaf () { return true; }

        public int getChildCount() { return 0; }
        
        public int getLeafCount() { return count; }

        public void addKeyValue(Object key, Object value)
            throws DuplicateException
        {
            String msg = "Duplicate detected: <" +
                key.toString() + ", " + value.toString() + ">";

            throw new DuplicateException(msg);
        }

        public void addTuple(Object tuple) throws DuplicateException
        {
            if ( !tuple.equals(value) ) {
                String msg = "Invalid duplicate " + value.toString() +
                    " vs. " + tuple.toString();
                throw new DuplicateException(msg);
            }
            
            if ( !allowDuplicates ) {
                String msg = "Duplicate detected: " + value.toString();
                throw new DuplicateException(msg);
            }
            
            ++count;
        }
    }
}
