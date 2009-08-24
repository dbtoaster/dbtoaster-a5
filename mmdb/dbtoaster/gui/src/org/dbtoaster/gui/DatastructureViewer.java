package org.dbtoaster.gui;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.jface.viewers.ILabelProvider;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.graphics.Image;

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
        
        boolean contains(Object dimVal);

        public void addTuple(Object tuple)
            throws TreeException, DuplicateException;

        public void addKeyValue(Object key, Object value)
            throws TreeException, DuplicateException;
    }

    enum DatastructureType { VARIABLE, LIST, MAP, SET };

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
        
        public void cover(Object point)
        {
            T p = (T) point;
            if ( lower.compareTo(p) > 0 ) lower = p;
            else if ( upper.compareTo(p) < 0 ) upper = p;
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
            Integer lower = (((Integer) key) / span)*span;
            Integer upper = lower + span;
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
            Long lower = (((Long) key) / span)*span;
            Long upper = lower + span;
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
            Float lower = Math.round((Float) key / span) * span;
            Float upper = lower + span;
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
            Double lower = Math.floor(((Double) key) / span) * span;
            Double upper = lower + span;
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

    class DatastructureNode implements ViewerNode
    {
        String dsName;
        DatastructureType dsType;
        Class<?> dsClassType;
        Class<?> dsKeyClassType;

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
        public String getTextLP() {
            return dsName + (dsRoot == null? "" : " " + dsRoot.getTextLP());
        }

        // Tree methods.
        public DatastructureNode(String name, DatastructureType type,
                Class<?> dsClass, Class<?> keyClass)
        {
            dsName = name;
            dsType = type;
            dsClassType = dsClass;
            dsKeyClassType = keyClass;

            Interval<?> initBucket = null;
            LinkedList<Field> dimensions = new LinkedList<Field>();
            LinkedList<BucketStrategy<?>> dimensionBuckets =
                new LinkedList<BucketStrategy<?>>();
            
            try
            {
                boolean boxed = isBoxedPrimitive(dsKeyClassType);
                boolean string = dsKeyClassType.equals(String.class);
                if ( dsKeyClassType.isPrimitive() || boxed || string )
                {
                    Class<?> c = (boxed || string?
                        dsKeyClassType : getClassOfPrimitive(dsKeyClassType));
                    dimensionBuckets.add(getBucketStrategy(c));
                    if ( string )
                        initBucket = dimensionBuckets.getLast().getBucket(c.newInstance());
                    else {
                        Constructor<?> strConstructor = c.getConstructor(String.class);
                        Object defaultFieldVal = strConstructor.newInstance("0");
                        initBucket = dimensionBuckets.getLast().getBucket(defaultFieldVal);
                    }
                }
                else
                {
                    for (Field f : dsKeyClassType.getFields())
                    {
                        if ( !Modifier.isStatic(f.getModifiers()) )
                        {
                            System.out.println("Adding dimension " +
                                f.getName() + " " + f.getType().getName());
    
                            Class<?> fType = f.getType().isPrimitive()?
                                getClassOfPrimitive(f.getType()) : f.getType();
    
                            dimensions.add(f);
                            dimensionBuckets.add(getBucketStrategy(fType));
                            
                            boolean fString = fType.equals(String.class);
    
                            if ( initBucket == null )
                            {
                                Object defaultFieldVal;
                                if ( fString ) defaultFieldVal = fType.newInstance();
    
                                else {
                                    Constructor<?> strConstructor =
                                        fType.getConstructor(String.class);
                                    defaultFieldVal = strConstructor.newInstance("0");
                                }
                                    
                                initBucket = dimensionBuckets.getLast().getBucket(defaultFieldVal);
                            }
                        }
                    }
                }

                dsRoot = new TreeNode(dsType == DatastructureType.LIST,
                    initBucket, dimensions, dimensionBuckets);

            } catch (Exception e) {
                System.out.println("Failed to create root for " + name);
                e.printStackTrace();
            }
            
            if ( dsRoot != null )
                System.out.println("Successfully created datastructure viewer for " + dsName);
        }
        
        private boolean isBoxedPrimitive(Class<?> cl)
        {
            return (
                cl.getName().equals(Void.class.getName()) ||
                cl.getName().equals(Boolean.class.getName()) ||
                cl.getName().equals(Byte.class.getName()) ||
                cl.getName().equals(Character.class.getName()) ||
                cl.getName().equals(Short.class.getName()) ||
                cl.getName().equals(Integer.class.getName()) ||
                cl.getName().equals(Float.class.getName()) ||
                cl.getName().equals(Long.class.getName()) ||
                cl.getName().equals(Double.class.getName())
            );
        }
        
        private Class<?> getClassOfPrimitive(Class<?> primitiveType)
        {
            Class<?> r = null;

            if ( primitiveType.equals(Void.TYPE) ) {
                r = Void.class;
            }
            else if ( primitiveType.equals(Boolean.TYPE) ) {
                r = Boolean.class;
            }
            else if ( primitiveType.equals(Byte.TYPE) ) {
                r = Byte.class;
            }
            else if ( primitiveType.equals(Character.TYPE) ) {
                r = Character.class;
            }
            else if ( primitiveType.equals(Short.TYPE) ) {
                r = Short.class;
            }
            else if ( primitiveType.equals(Integer.TYPE) ) {
                r = Integer.class;
            }
            else if ( primitiveType.equals(Float.TYPE) ) {
                r = Float.class;
            }
            else if ( primitiveType.equals(Long.TYPE) ) {
                r = Long.class;
            }
            else if ( primitiveType.equals(Double.TYPE) ) {
                r = Double.class;
            }
            else
                System.err.println("Invalid primitive type " + primitiveType.getName());
            
            return r;
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
            else if ( fieldType.equals(Integer.TYPE)
                        || fieldType.equals(Integer.class) )
            {
                r = new FixedIntegerBucket(DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else if ( fieldType.equals(Long.TYPE)
                    || fieldType.equals(Long.class) )
            {
                r = new FixedLongBucket((long) DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else if ( fieldType.equals(Float.TYPE)
                    || fieldType.equals(Float.class) )
            {
                r = new FixedFloatBucket((float) DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else if ( fieldType.equals(Double.TYPE)
                    || fieldType.equals(Double.class) )
            {
                r = new FixedDoubleBucket((double) DEFAULT_NUMERIC_BUCKET_WIDTH);
            }
            else if ( fieldType.equals(String.class) )
            {
                r = new FixedStringBucket(DEFAULT_STRING_BUCKET_PREFIX_LENGTH);
            }
            else
                System.err.println("Invalid primitive type " + fieldType.getName());
            
            return r;
        }

        public boolean isLeaf() { return dsRoot.isLeaf(); }

        public int getChildCount() { return dsRoot.getChildCount(); }
        
        public int getLeafCount() { return dsRoot.getLeafCount(); }
        
        public boolean contains(Object dimVal) { return dsRoot.contains(dimVal); }

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
            if ( parentValues != null ) {
                for (Interval<?> i : parentValues) r += i.toString();
            }
            if ( bucket != null ) r += bucket.toString();
            r += " (" + getLeafCount() + " items)";
            return r;
        }

        // Tree methods.

        // Root node constructor
        public TreeNode(boolean duplicates, Interval<?> bucket,
            LinkedList<Field> stDimensions, LinkedList<BucketStrategy<?>> bs)
            throws TreeException, DuplicateException
        {
            initRoot(duplicates, bucket, stDimensions, bs);
        }

        // Incremental constructors
        public TreeNode(boolean duplicates, ViewerNode par,
                LinkedList<Interval<?>> parValues,
                LinkedList<Field> stDimensions,
                LinkedList<BucketStrategy<?>> bs, Object tuple)
            throws TreeException, DuplicateException
        {
            init(duplicates, par, parValues, tuple, stDimensions, bs);
            addTuple(tuple);
        }

        public TreeNode(boolean duplicates, ViewerNode par,
                LinkedList<Interval<?>> parValues,
                LinkedList<Field> stDimensions,
                LinkedList<BucketStrategy<?>> bs,
                Object key, Object value)
            throws TreeException, DuplicateException
        {
            init(duplicates, par, parValues, key, stDimensions, bs);
            addKeyValue(key, value);
        }

        void initRoot(boolean duplicates, Interval<?> bucket,
                LinkedList<Field> stDimensions,
                LinkedList<BucketStrategy<?>> strategies)
            throws TreeException
        {
            allowDuplicates = duplicates;
            parent = null;
            parentValues = null;
            this.bucket = bucket;
    
            children = new TreeMap<Object, ViewerNode>();
            
            if ( stDimensions.size() > 0 && (stDimensions.size() != strategies.size()) )
            {
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
                String msg = "Found no dimensions, assuming object as key.";
                System.out.println(msg);
    
                childDimension = null;
                childDimensionType = this.bucket.getLower().getClass();
                
                subTreeDimensions = null;
                subTreeBucketing = null;
    
                bucketStrategy = strategies.getFirst();
            }
        }
            
        void init(boolean duplicates,
                ViewerNode par, LinkedList<Interval<?>> parValues,
                Object value, LinkedList<Field> stDimensions,
                LinkedList<BucketStrategy<?>> strategies)
            throws TreeException
        {
            allowDuplicates = duplicates;
            parent = par;
            parentValues = parValues;

            children = new TreeMap<Object, ViewerNode>();
            
            if ( stDimensions.size() > 0 && (stDimensions.size() != strategies.size()) )
            {
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
                String msg = "Found no dimensions, assuming object as key.";
                System.out.println(msg);

                childDimension = null;
                childDimensionType = this.bucket.getLower().getClass();
                
                subTreeDimensions = null;
                subTreeBucketing = null;

                bucketStrategy = strategies.getFirst();
            }
            
            try {
                bucket = bucketStrategy.getBucket(getBucketDimension(value));
            } catch (IntervalException e) {
                String msg = "Interval: " + e.getMessage();
                e.printStackTrace();
                throw new TreeException(msg);
            }
        }
        
        public boolean contains(Object dimVal)
        {
            return bucket.contains(dimVal);
        }

        Object getBucketDimension(Object value) throws TreeException
        {
            Object childDimVal;
            
            if ( childDimension != null )
            {
                try {
                    childDimVal = childDimension.get(value);
                } catch (Exception e) {
                    String msg = "Failed to get child field " +
                        childDimension.getName() + " from " +
                        value.getClass().getName();
    
                    e.printStackTrace();
                    throw new TreeException(msg);
                }
            }
            else {
                childDimVal = value;
            }

           return childDimVal;
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
            Object dimVal = getBucketDimension(tuple);
            
            System.out.println("Handling dim val: " + dimVal + " at: " + bucket + " children: " + children.size());

            if ( parent == null && !contains(dimVal) ) {
                System.out.println("Covering " + dimVal);
                bucket.cover(dimVal);
            }

            Object childDimVal = children.ceilingKey(dimVal);
            ViewerNode childNode = null;
            
            if ( subTreeDimensions == null )
            {
                childNode = children.get(dimVal);
                if ( childNode == null ) {
                    System.out.println("Adding leaf child " + dimVal + " for " + bucket.toString());
                    children.put(dimVal, new MapElementNode(allowDuplicates, tuple));
                }
                else {
                    System.out.println("Adding to existing leaf child " + dimVal + " for " + bucket.toString());
                    childNode.addTuple(tuple);
                }
            }
            else if ( childDimVal == null  || !bucket.contains(dimVal) )
            {
                try {
                    System.out.println("Adding new subtree " + childNode + " "
                        + (childNode != null ? childNode.contains(dimVal) : ""));
                    
                    Interval<?> newChildBucket =
                        bucketStrategy.getBucket(dimVal);
                    
                    TreeNode newSubTree = new TreeNode(
                        allowDuplicates, this, getNodeBucket(),
                        subTreeDimensions, subTreeBucketing, tuple);

                    System.out.println("Adding new child at " + newChildBucket.getUpper() + " to " + bucket.toString());
                    children.put(newChildBucket.getUpper(), newSubTree);

                } catch (IntervalException e) {
                    e.printStackTrace();
                }
            }
            else {
                System.out.println("Adding to existing child " + childDimVal + " for " + bucket.toString());
                childNode = children.get(childDimVal);
                childNode.addTuple(tuple);
            }
            
            
            System.out.println("Done with dim val: " + dimVal + " at: " + bucket + " children: " + children.size());

        }

        public void addKeyValue(Object key, Object value)
            throws TreeException, DuplicateException
        {
            Object dimVal = getBucketDimension(key);

            if ( parent == null && !contains(dimVal) ) {
                System.out.println("Covering " + dimVal);
                bucket.cover(dimVal);
            }
            
            Object childDimVal = children.ceilingKey(dimVal);
            ViewerNode childNode = null;

            if ( subTreeDimensions == null )
            {
                childNode = children.get(dimVal);
                if ( childNode == null ) {
                    System.out.println("Adding leaf child " + dimVal);
                    children.put(dimVal, new MapElementNode(key, value));
                }
                else {
                    System.out.println("Adding to existing leaf child " + dimVal);
                    childNode.addKeyValue(key, value);
                }
            }
            else if ( childDimVal == null || !bucket.contains(dimVal) )
            {
                try {
                    Interval<?> newChildBucket =
                        bucketStrategy.getBucket(dimVal);

                    TreeNode newSubTree = new TreeNode(
                        allowDuplicates, this, getNodeBucket(),
                        subTreeDimensions, subTreeBucketing, key, value);

                    System.out.println("Adding subtree child " + newChildBucket.getUpper());
                    children.put(newChildBucket.getUpper(), newSubTree);
                } catch (IntervalException e) {
                    e.printStackTrace();
                }
            }
            else {
                childNode = children.get(childDimVal);
                childNode.addKeyValue(key, value);
            }
        }
        
        public String toString() { return bucket.toString() + ", " + children.size(); }
        
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

        public boolean contains(Object dimVal)
        {
            return dimVal.equals(value);
        }

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

    class DatastructureContentProvider implements ITreeContentProvider
    {

        public Object[] getChildren(Object parentElement)
        {
            return ((ViewerNode) parentElement).getChildrenCP();
        }

        public Object getParent(Object element)
        {
            return ((ViewerNode) element).getParentCP();
        }

        public boolean hasChildren(Object element)
        {
            return ((ViewerNode) element).hasChildrenCP();
        }

        // Return the root ranges of all data structures.
        public Object[] getElements(Object inputElement)
        {
            return ((LinkedList<DatastructureNode>) inputElement).toArray();
        }

        public void dispose() { }

        public void inputChanged(Viewer viewer, Object oldInput, Object newInput)
        {}

    }

    class DatastructureLabelProvider implements ILabelProvider
    {

        public Image getImage(Object element)
        {
            return null;
        }

        public String getText(Object element)
        {
            return ((ViewerNode) element).getTextLP();
        }

        public boolean isLabelProperty(Object element, String property)
        {
            return false;
        }

        public void addListener(ILabelProviderListener listener) {}

        public void removeListener(ILabelProviderListener listener) {}
        
        public void dispose() {}
    }
}
