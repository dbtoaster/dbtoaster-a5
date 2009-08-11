package org.dbtoaster.gui;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;

import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.Debugger;
import org.dbtoaster.model.Query;
import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ILabelProvider;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TextCellEditor;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Item;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.IPerspectiveDescriptor;
import org.eclipse.ui.IPerspectiveListener;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.part.ViewPart;

public class DebugView extends ViewPart
{
    public final static String ID = "dbtoaster_gui.debugview";

    DBToasterWorkspace dbtWorkspace;

    String perspectiveId;
    int dqQueriesHash;

    TableViewer stepInputViewer;
    DebugQueryPanel dqPanel;
    
    Query currentQuery;
    Debugger currentDebugger;

    final static String[] properties = { "Relation", "Values" };
    LinkedList<StepInput> stepInputs;
    LinkedList<DataStructureRange> dataStructures;

    class DebugQueryPanel
    {
        Tree dqTree;
        DBToasterWorkspace dbtWorkspace;

        DebugQueryPanel(Composite parent, DBToasterWorkspace dbtw)
        {
            dbtWorkspace = dbtw;
            Composite dqComp = new Composite(parent, SWT.NONE);
            dqComp.setLayout(new GridLayout());
            GridData dqcLD = new GridData(SWT.FILL, SWT.FILL, false, true);
            dqComp.setLayoutData(dqcLD);
            
            Label dqLabel = new Label(dqComp, SWT.NONE);
            dqLabel.setText("Debugger queries:");
            GridData dqlbLD = new GridData(SWT.FILL, SWT.FILL, true, false);
            dqLabel.setLayoutData(dqlbLD);

            dqTree = new Tree(dqComp, SWT.SINGLE | SWT.BORDER);
            GridData dqtLD = new GridData(SWT.FILL, SWT.FILL, true, true);
            dqtLD.widthHint = 200;
            dqTree.setLayoutData(dqtLD);
            dqTree.addSelectionListener(new DQSelectionListener());
            
            redraw();
        }
        
        void redraw()
        {
            dqTree.removeAll();
            
            LinkedList<Query> debugQueries = dbtWorkspace.getDebuggingQueries();
            dqQueriesHash = debugQueries.hashCode();

            System.out.println("DebugView found " + debugQueries.size() + " debugger binaries.");

            for (Query q : debugQueries)
            {
                TreeItem qItem = new TreeItem(dqTree, SWT.NONE);
                qItem.setText(q.getQueryName());
            }
        }
        
        class DQSelectionListener implements SelectionListener
        {
            public void widgetSelected(SelectionEvent e)
            {
                TreeItem[] selected = dqTree.getSelection();

                boolean clear = true;

                if ( selected.length > 0 )
                {
                    TreeItem sel = selected[0];

                    currentQuery = dbtWorkspace.getQuery(sel.getText());
                    if ( currentQuery != null )
                    {
                        currentDebugger = currentQuery.getDebugger();

                        // Load all step method inputs.
                        if ( currentDebugger != null )
                        {
                            int numStepInputs = 0;
                            stepInputs.clear();
                            
                            LinkedList<String> inputs =
                                currentDebugger.getInputRelationNames();
                            
                            for (String rel : inputs)
                            {
                                Class<?> debugClass =
                                    currentDebugger.getStepDebugTupleType(rel);

                                Class<?> dataClass =
                                    currentDebugger.getStepDataTupleType(rel);

                                int streamId = currentDebugger.getStreamId(rel);

                                if ( !(debugClass == null || dataClass == null
                                        || streamId < 0) )
                                {
                                    StepInput n = new StepInput(
                                        rel, debugClass, dataClass, streamId);
    
                                    stepInputs.add(n);
                                    ++numStepInputs;
                                }
                                else {
                                    String msg = "Failed to get step metadata for " +
                                        rel + " " + debugClass + " " + dataClass + " " + streamId;
                                    System.out.println(msg);
                                }
                            }
                            
                            // Refresh the step input viewer
                            System.out.println("Refreshing input viewer with "
                                    + numStepInputs + " step inputs");
                            stepInputViewer.refresh();
                            
                            clear = false;
                        }
                    }
                }
                
                if ( clear ) {
                    stepInputs.clear();
                    stepInputViewer.refresh();
                }
            }
            
            public void widgetDefaultSelected(SelectionEvent e) {}
        }
    
    }

    public void createPartControl(Composite parent)
    {
        dbtWorkspace = DBToasterWorkspace.getWorkspace();

        Composite top = new Composite(parent, SWT.NONE);
        top.setLayout(new GridLayout(3, false));

        // Query selection panel.
        dqPanel = new DebugQueryPanel(top, dbtWorkspace);

        // Specify event
        Composite eventSpec = new Composite(top, SWT.BORDER);
        eventSpec.setLayout(new GridLayout());
        GridData esData = new GridData(SWT.FILL, SWT.FILL, false, true);
        esData.widthHint = 270;
        esData.heightHint = 250;
        eventSpec.setLayoutData(esData);

        Label eventSpecLabel = new Label(eventSpec, SWT.NONE);
        GridData eslData = new GridData(SWT.FILL, SWT.FILL, true, false);
        eslData.heightHint = 30;
        eventSpecLabel.setText("Specify trace delta");
        eventSpecLabel.setLayoutData(eslData);

        // Table of relations
        stepInputViewer = new TableViewer(eventSpec, SWT.FULL_SELECTION);
        stepInputViewer.setContentProvider(new StepInputContentProvider());
        stepInputViewer.setLabelProvider(new StepInputLabelProvider());

        stepInputs = new LinkedList<StepInput>();
        stepInputViewer.setInput(stepInputs);

        Table table = stepInputViewer.getTable();
        GridData tlData = new GridData(GridData.FILL_BOTH);
        table.setLayoutData(tlData);

        TableColumn relDesc = new TableColumn(table, SWT.LEFT);
        relDesc.setText("Relation");

        TableColumn values = new TableColumn(table, SWT.LEFT);
        values.setText("Enter values");

        relDesc.pack();
        values.pack();

        // Set default widths.
        table.getColumn(0).setWidth(100);
        table.getColumn(1).setWidth(100);

        table.setHeaderVisible(true);
        table.setLinesVisible(true);

        CellEditor[] editors = new CellEditor[2];
        editors[0] = new TextCellEditor(table, SWT.READ_ONLY);
        editors[1] = new TextCellEditor(table);

        stepInputViewer.setColumnProperties(properties);
        stepInputViewer.setCellModifier(new StepInputModifier(stepInputViewer));
        stepInputViewer.setCellEditors(editors);

        Composite applyEventComp = new Composite(eventSpec, SWT.NONE);
        applyEventComp.setLayout(new GridLayout(3, false));
        GridData aecData = new GridData(SWT.FILL, SWT.FILL, true, false);
        aecData.heightHint = 40;
        applyEventComp.setLayoutData(aecData);

        // Insert button
        Label eventTypeLabel = new Label(applyEventComp, SWT.NONE);
        GridData etlData = new GridData(SWT.CENTER, SWT.CENTER, true, false);
        eventTypeLabel.setText("Event: ");
        eventTypeLabel.setLayoutData(etlData);

        Button insertButton = new Button(applyEventComp, SWT.PUSH);
        insertButton.setText("Insert");
        insertButton.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, true,
                false));

        // Delete button
        Button deleteButton = new Button(applyEventComp, SWT.PUSH);
        deleteButton.setText("Delete");
        deleteButton.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, true,
                false));

        Composite debugEventComp = new Composite(eventSpec, SWT.NONE);
        debugEventComp.setLayout(new GridLayout(3, false));
        GridData decData = new GridData(SWT.FILL, SWT.FILL, true, false);
        decData.heightHint = 40;
        debugEventComp.setLayoutData(decData);

        // Break button
        Button breakButton = new Button(debugEventComp, SWT.TOGGLE);
        GridData bbData = new GridData(SWT.CENTER, SWT.CENTER, false, false);
        breakButton.setText("x Break");
        breakButton.setLayoutData(bbData);

        // Step button
        Button stepButton = new Button(debugEventComp, SWT.PUSH);
        GridData sbData = new GridData(SWT.CENTER, SWT.CENTER, false, false);
        stepButton.setText(">> Step");
        stepButton.setLayoutData(sbData);

        // Step to button
        Button stepToButton = new Button(debugEventComp, SWT.TOGGLE);
        GridData stbData = new GridData(SWT.CENTER, SWT.CENTER, false, false);
        stepToButton.setText("--> Step to");
        stepToButton.setLayoutData(stbData);

        // Data structure vis
        TreeViewer dataStructViewer = new TreeViewer(top, SWT.NONE);
        dataStructViewer.setContentProvider(new DataStructureContentProvider());
        dataStructViewer.setLabelProvider(new DataStructureLabelProvider());

        dataStructures = new LinkedList<DataStructureRange>();
        buildVwapDataStructures();
        dataStructViewer.setInput(dataStructures);

        Tree dsvTree = dataStructViewer.getTree();
        GridData dsvData = new GridData(SWT.FILL, SWT.FILL, true, true);
        dsvTree.setLayoutData(dsvData);

        perspectiveId = getViewSite().getPage().getPerspective().getId();

        getViewSite().getWorkbenchWindow().addPerspectiveListener(
            new IPerspectiveListener()
            {
                public void perspectiveChanged(IWorkbenchPage page,
                        IPerspectiveDescriptor perspective, String changeId)
                {}

                public void perspectiveActivated(IWorkbenchPage page,
                        IPerspectiveDescriptor perspective)
                {
                    int queriesHash = dbtWorkspace.getDebuggingQueries().hashCode();

                    System.out.println("DQ hash comp: " + queriesHash + ", " + dqQueriesHash);

                    if (perspective.getId().equals(perspectiveId)
                            && dqQueriesHash != queriesHash)
                    {
                        System.out.println("Redrawing dqPanel");
                        dqPanel.redraw();
                    }
                }
            });

    }

    public void setFocus() {}

    void buildVwapDataStructures()
    {
        try
        {
            DataStructureRange qpminRoot = new DataStructureRange("q[pmin]");
            DataStructureRange qc1 = qpminRoot.addChildRange(160000, 180000);
            DataStructureRange qc2 = qpminRoot.addChildRange(180000, 200000);
            DataStructureRange qc3 = qpminRoot.addChildRange(200000, 220000);

            DataStructureRange qc11 = qc1.addChildRange(160000, 170000);
            DataStructureRange qc12 = qc1.addChildRange(170000, 180000);
            qc11.addChildValue(163420);
            qc12.addChildValue(177800);
            qc2.addChildValue(185100);
            qc3.addChildValue(205700);

            DataStructureRange mp2Root = new DataStructureRange("m[p2]");
            DataStructureRange mc1 = mp2Root.addChildRange(160000, 190000);
            DataStructureRange mc2 = mp2Root.addChildRange(190000, 220000);
            mc1.addChildValue(163420);
            mc1.addChildValue(177800);
            mc1.addChildValue(185100);
            mc2.addChildValue(205700);

            DataStructureRange sv0 = new DataStructureRange("sv0", 0);

            DataStructureRange sv1Root = new DataStructureRange("sv1[p2]");
            DataStructureRange vc1 = sv1Root.addChildRange(160000, 180000);
            DataStructureRange vc2 = sv1Root.addChildRange(180000, 220000);
            vc1.addChildValue(163420);
            vc1.addChildValue(177800);
            vc2.addChildValue(185100);
            vc2.addChildValue(205700);

            dataStructures.add(qpminRoot);
            dataStructures.add(mp2Root);
            dataStructures.add(sv1Root);
            dataStructures.add(sv0);

        } catch (DataStructureRangeException e)
        {
            e.printStackTrace();
        }

    }

    // Helper classes
    
    // Step tuple input.
    class StepInput
    {
        private String relationName;
        private LinkedHashMap<String, Class<?>> relationSchema;
        
        private Class<?> debuggerInputType;
        private Class<?> inputType;
        private int inputStreamId;
        private Object inputValue;

        public StepInput() {
            relationSchema = new LinkedHashMap<String, Class<?>>();
        }
        
        public StepInput(String relName, Class<?> dInType, Class<?> inType,
                int inStreamId)
        {
            relationName = relName;
            debuggerInputType = dInType;
            inputType = inType;
            inputStreamId = inStreamId;
            
            validateDebuggerTuple();

            relationSchema = new LinkedHashMap<String, Class<?>>();
            for (Field f : inputType.getFields())
            {
                if ( !Modifier.isStatic(f.getModifiers()) ) {
                    Class<?> fType = f.getType();
                    if ( fType.isPrimitive() ) {
                        fType = getClassOfPrimitive(fType);
                    }
                    relationSchema.put(f.getName(), fType);
                }
            }

            try {
                inputValue = inputType.newInstance();
            } catch (Exception e) {
                String msg = "Error instantiating step input for " + relationName;
                System.err.println(msg);
                e.printStackTrace();
            }
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
            else if ( primitiveType.equals(Character.TYPE) ) {
                r = Character.class;
            }
            else if ( primitiveType.equals(Byte.TYPE) ) {
                r = Byte.class;
            }
            else if ( primitiveType.equals(Short.TYPE) ) {
                r = Short.class;
            }
            else if ( primitiveType.equals(Integer.TYPE) ) {
                r = Integer.class;
            }
            else if ( primitiveType.equals(Long.TYPE) ) {
                r = Long.class;
            }
            else if ( primitiveType.equals(Float.TYPE) ) {
                r = Float.class;
            }
            else if ( primitiveType.equals(Double.TYPE) ) {
                r = Double.class;
            }
            else
                System.err.println("Invalid primitive type " + primitiveType.getName());
            
            return r;
        }

        private void validateDebuggerTuple()
        {
            try
            {
                if ( !debuggerInputType.getField("type").getDeclaringClass().
                        getName().equals(debuggerInputType.getName()) )
                {
                    String msg = "No 'type' field in debugger tuple for " + relationName;
                    System.err.println(msg);
                    for (Field f : debuggerInputType.getFields())
                        System.err.println(debuggerInputType.getName() + " fields: " + f.getName());
                }

                if ( !debuggerInputType.getField("id").getDeclaringClass().
                        getName().equals(debuggerInputType.getName()) )
                {
                    String msg = "No 'id' field in debugger tuple for " + relationName;
                    System.err.println(msg);
                    for (Field f : debuggerInputType.getFields())
                        System.err.println(debuggerInputType.getName() + " fields: " + f.getName());
                }

            } catch (SecurityException e) {
                String msg = "SecurityException retrieving debugger tuple fields in " + relationName;
                System.err.println(msg);
                e.printStackTrace();
            } catch (NoSuchFieldException e) {
                String msg = "Unable to find debugger tuple fields " + relationName;
                System.err.println(msg);
                e.printStackTrace();
            }
        }

        public int getNumFields() { return relationSchema.size(); }

        public void setValues(LinkedList<String> fields)
        {
            if ( fields.size() != relationSchema.size() )
            {
                System.err.println("Invalid number of fields for " + relationName);
                return;
            }

            int i = 0;
            for (Map.Entry<String, Class<?>> e : relationSchema.entrySet())
            {
                Exception ex = null;
                String status = null;

                boolean setField = false;
                String fStrVal = fields.get(i);
                Class<?> valClassType = e.getValue();
                try {
                    Constructor<?> valCstr =
                        valClassType.getConstructor(fStrVal.getClass());
                    Object fVal = valCstr.newInstance(fStrVal);
                    inputType.getField(e.getKey()).set(inputValue, fVal);
                    setField = true;

                } catch (SecurityException e1) {
                    status =
                        "SecurityException invoking String constructor for " +
                            valClassType.getName();
                    ex = e1;
                } catch (NoSuchMethodException e1) {
                    status = "No String constructor found for " + valClassType.getName();
                    ex = e1;
                } catch (IllegalArgumentException e1) {
                    status = "Illegal argument with " + fStrVal + ", " + valClassType.getName();
                    ex = e1;
                } catch (InstantiationException e1) {
                    status = "Instantiation failed on " + fStrVal + ", " + valClassType.getName();
                    ex = e1;
                } catch (InvocationTargetException e1) {
                    status = "String constructor failed for " + valClassType.getName(); 
                    ex = e1;
                } catch (IllegalAccessException e1) {
                    status = "Illegal access to " + e.getKey() + ", " + inputType.getName();
                    ex = e1;
                } catch (NoSuchFieldException e1) {
                    status = "No field " + e.getKey() + " in " + inputType.getName();
                    ex = e1;
                }

                if ( !setField )
                {
                    System.err.println(status);
                    if ( ex != null ) ex.printStackTrace();
                    return;
                }

                ++i;
            }
            
            System.out.println("Set input value to " + inputValue.toString());
        }
        
        public String getNameAndSchema()
        {
            String r = "";
            for (String f : relationSchema.keySet())
                r += ((r.isEmpty()? "" : ",") + f);
            r = relationName + "(" + r + ")";
            return r;
        }
        
        public String getValueString()
        {
            String r = "";
            for (Map.Entry<String, Class<?>> e : relationSchema.entrySet())
            {
                try {
                    Object f = inputType.getField(e.getKey()).get(inputValue);
                    r += ((r.isEmpty() ? "" : ",") + f.toString()); 
                } catch (NoSuchFieldException e1) {
                    String msg = "Invalid schema for " + relationName +
                        " (no field " + e.getKey() + ")";
                    System.err.println(msg);
                    e1.printStackTrace();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
            
            return r;
        }

        public Object getDebuggerTuple(int eventType)
        {
            Object r = null;
            try
            {
                Constructor<?> cstr = debuggerInputType.getConstructor(
                    Integer.class, Integer.class, inputType);
                r = cstr.newInstance(eventType, inputStreamId, inputValue);
            } catch (Exception e) {
                System.err.println(
                    "Failed to create debugger tuple: " + e.getMessage());
                e.printStackTrace();
            }
            
            return r;
        }
    }
    
    class StepInputContentProvider implements IStructuredContentProvider
    {

        public Object[] getElements(Object inputElement)
        {
            return ((LinkedList<String>) inputElement).toArray();
        }

        public void dispose() {}

        public void inputChanged(Viewer v, Object oldIn, Object newIn) {}

    }

    class StepInputLabelProvider implements ITableLabelProvider
    {

        public Image getColumnImage(Object element, int columnIndex)
        {
            return null;
        }

        public String getColumnText(Object element, int columnIndex)
        {
            StepInput r = (StepInput) element;
            String result = new String();
            switch (columnIndex) {
            case 0:
                result = r.getNameAndSchema();
                break;
            case 1:
                result = r.getValueString();
                break;
            default:
                break;
            }
            return result;
        }

        public void dispose() {}

        public boolean isLabelProperty(Object element, String property)
        {
            return false;
        }

        public void addListener(ILabelProviderListener listener) { }

        public void removeListener(ILabelProviderListener listener) {}

    }

    class StepInputModifier implements ICellModifier
    {

        private Viewer viewer;

        public StepInputModifier(Viewer v)
        {
            viewer = v;
        }

        public boolean canModify(Object element, String property)
        {
            return property.equals(properties[1]);
        }

        public Object getValue(Object element, String property)
        {
            StepInput r = (StepInput) element;
            if (property.equals(properties[0])) return r.getNameAndSchema();
            else if (property.equals(properties[1])) return r.getValueString();

            return null;
        }

        public void modify(Object element, String property, Object value)
        {
            if (element instanceof Item) element = ((Item) element).getData();

            StepInput r = (StepInput) element;
            if (property.equals(properties[0]))
            {
                // No modifications allowed.
                return;
            }

            else if (property.equals(properties[1]))
            {
                String newVals = (String) value;
                StringTokenizer splitter = new StringTokenizer(newVals, ",");

                LinkedList<String> values = new LinkedList<String>();
                while (splitter.hasMoreTokens())
                    values.add(splitter.nextToken());

                if (values.size() == r.getNumFields())
                    r.setValues(values);
                else {
                    System.out.println("Insufficient fields provided (need " +
                            r.getNumFields() + ")");
                }
            }

            viewer.refresh();
        }

    }

    // Map visualization.
    
    class DataStructureRangeException extends Exception
    {
        private static final long serialVersionUID = -8676531280324140041L;

        public DataStructureRangeException()
        {
        }

        public DataStructureRangeException(String msg)
        {
            super(msg);
        }
    };

    // TODO: make fully dynamic, i.e. remove/update
    class DataStructureRange
    {
        String dsName;
        Boolean wholeRange;
        Boolean singleValue;

        // TODO: generalize for arbitrary type map keys.
        Integer lowerBound;
        Integer upperBound;
        Integer keyValue;

        DataStructureRange parent;
        private LinkedList<DataStructureRange> children;

        public DataStructureRange(String name)
        {
            dsName = name;
            wholeRange = true;
            singleValue = false;
            parent = null;
            children = new LinkedList<DataStructureRange>();
        }

        public DataStructureRange(String name, Integer kv)
        {
            dsName = name;
            wholeRange = false;
            singleValue = true;
            keyValue = kv;
            parent = null;
        }

        public DataStructureRange(String name, DataStructureRange p,
                Integer lb, Integer ub)
        {
            wholeRange = false;
            singleValue = false;
            lowerBound = lb;
            upperBound = ub;
            parent = p;
            children = new LinkedList<DataStructureRange>();
        }

        public DataStructureRange(String name, DataStructureRange p, Integer kv)
        {
            dsName = name;
            wholeRange = false;
            singleValue = true;
            keyValue = kv;
            parent = p;
        }

        public String getName()
        {
            return dsName;
        }

        public Boolean getIsWholeRange()
        {
            return wholeRange;
        }

        public Boolean getIsSingleValue()
        {
            return singleValue;
        }

        public void setWholeRange(Boolean wr)
        {
            if (singleValue && wr) singleValue = false;
            wholeRange = wr;
        }

        public void setSingleValue(Boolean sv)
        {
            if (wholeRange && sv) wholeRange = false;
            singleValue = sv;
        }

        public Integer getLowerBound()
        {
            if (!wholeRange) return lowerBound;
            else return null;
        }

        public Integer getUpperBound()
        {
            if (!wholeRange) return upperBound;
            else return null;
        }

        public void setLowerBound(Integer lb)
        {
            if (!wholeRange) lowerBound = lb;
        }

        public void setUpperBound(Integer ub)
        {
            if (!wholeRange) upperBound = ub;
        }

        public Integer getValue()
        {
            return singleValue ? keyValue : null;
        }

        public void setValue(Integer kv)
        {
            if (singleValue) keyValue = kv;
        }

        public DataStructureRange getParent()
        {
            return parent;
        }

        public LinkedList<DataStructureRange> getChildren()
        {
            return children;
        }

        public DataStructureRange addChildRange(Integer clb, Integer cub)
                throws DataStructureRangeException
        {
            if (singleValue)
                throw new DataStructureRangeException(
                        "Values cannot have children.");

            // Check range validity
            if (clb >= cub)
                throw new DataStructureRangeException("Invalid input range.");

            if (!wholeRange && (clb < lowerBound || upperBound < cub))
                throw new DataStructureRangeException("Invalid child range.");

            // Check range disjointness
            Boolean overlap = false;
            for (DataStructureRange dsr : children)
            {
                if (!dsr.singleValue
                        && !(cub <= dsr.lowerBound || dsr.upperBound <= clb))
                {
                    overlap = true;
                    break;
                }
                else if (dsr.singleValue && clb <= dsr.keyValue
                        && dsr.keyValue <= cub)
                {
                    overlap = true;
                    break;
                }
            }

            if (overlap)
                throw new DataStructureRangeException(
                        "Overlapping child range.");

            DataStructureRange r = new DataStructureRange(dsName, this, clb,
                    cub);

            children.add(r);
            return r;
        }

        public DataStructureRange addChildValue(Integer kv)
                throws DataStructureRangeException
        {
            if (singleValue)
                throw new DataStructureRangeException(
                        "Values cannot have children.");

            // Check value containment
            if (!wholeRange && (kv < lowerBound || upperBound < kv))
                throw new DataStructureRangeException("Invalid child value.");

            // Check unique
            Boolean overlap = false;
            for (DataStructureRange dsr : children)
            {
                if (!dsr.singleValue
                        && (dsr.lowerBound <= kv && kv <= dsr.upperBound))
                {
                    overlap = true;
                    break;
                }
                else if (dsr.singleValue && dsr.keyValue.equals(kv))
                {
                    overlap = true;
                    break;
                }
            }

            if (overlap)
                throw new DataStructureRangeException(
                        "Overlapping child value.");

            DataStructureRange r = new DataStructureRange(dsName, this, kv);

            children.add(r);
            return r;
        }
    }

    class DataStructureContentProvider implements ITreeContentProvider
    {

        public Object[] getChildren(Object parentElement)
        {
            return ((DataStructureRange) parentElement).getChildren().toArray();
        }

        public Object getParent(Object element)
        {
            return ((DataStructureRange) element).getParent();
        }

        public boolean hasChildren(Object element)
        {
            LinkedList<DataStructureRange> c = ((DataStructureRange) element)
                    .getChildren();

            return c != null && c.size() > 0;
        }

        // Return the root ranges of all data structures.
        public Object[] getElements(Object inputElement)
        {
            return ((LinkedList<DataStructureRange>) inputElement).toArray();
        }

        public void dispose()
        {
        }

        public void inputChanged(Viewer viewer, Object oldInput, Object newInput)
        {
        }

    }

    class DataStructureLabelProvider implements ILabelProvider
    {

        public Image getImage(Object element)
        {
            return null;
        }

        public String getText(Object element)
        {
            DataStructureRange dsr = (DataStructureRange) element;
            String r;

            if (dsr.getIsWholeRange()) r = dsr.getName();

            else if (dsr.getIsSingleValue())
            {
                if (dsr.getParent() == null) r = dsr.getName();
                else r = "<" + dsr.getValue().toString() + ">";
            }

            else
            {
                r = "[" + dsr.getLowerBound().toString() + ","
                        + dsr.getUpperBound().toString() + "]";
            }
            return r;
        }

        public void addListener(ILabelProviderListener listener)
        {
        }

        public void dispose()
        {
        }

        public boolean isLabelProperty(Object element, String property)
        {
            return false;
        }

        public void removeListener(ILabelProviderListener listener)
        {
        }

    }
}
