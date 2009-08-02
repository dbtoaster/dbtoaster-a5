package org.dbtoaster.gui;

import java.util.LinkedList;
import java.util.StringTokenizer;

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
import org.eclipse.ui.part.ViewPart;

public class DebugView extends ViewPart
{

    public final static String ID = "dbtoaster_gui.debugview";

    final static String[] properties = { "Relation", "Values" };
    private LinkedList<Relation> relations;
    private LinkedList<DataStructureRange> dataStructures;

    public void createPartControl(Composite parent)
    {
        Composite top = new Composite(parent, SWT.NONE);
        top.setLayout(new GridLayout(2, false));

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
        TableViewer tv = new TableViewer(eventSpec, SWT.FULL_SELECTION);
        tv.setContentProvider(new RelationContentProvider());
        tv.setLabelProvider(new RelationLabelProvider());

        relations = new LinkedList<Relation>();
        buildVwapRelations();
        tv.setInput(relations);

        Table table = tv.getTable();
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

        tv.setColumnProperties(properties);
        tv.setCellModifier(new RelationModifier(tv));
        tv.setCellEditors(editors);

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

    }

    public void setFocus()
    {
    }

    void buildVwapRelations()
    {
        relations.add(new Relation("Bids(p int, v int)", "0,0"));
    }

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
    class Relation
    {
        private String nameAndSchema;
        private LinkedList<String> values;

        public Relation()
        {
        }

        public Relation(String ns)
        {
            nameAndSchema = ns;
        }

        public Relation(String ns, String v)
        {
            nameAndSchema = ns;
            StringTokenizer splitter = new StringTokenizer(v, ",");
            values = new LinkedList<String>();
            while (splitter.hasMoreTokens())
                values.add(splitter.nextToken());
        }

        public String getNameAndSchema()
        {
            return nameAndSchema;
        }

        public LinkedList<String> getValues()
        {
            return values;
        }

        public String getValueString()
        {
            String result = "";
            for (String v : values)
                result += (result.length() == 0 ? "" : ",") + v;
            return result;
        }

        public void setNameAndSchema(String s)
        {
            nameAndSchema = s;
        }

        public void setValues(LinkedList<String> v)
        {
            values = v;
        }

        public void addValue(String v)
        {
            if (values == null) values = new LinkedList<String>();
            values.add(v);
        }

        public String toString()
        {
            return nameAndSchema + ": " + getValueString();
        }
    }

    class RelationContentProvider implements IStructuredContentProvider
    {

        public Object[] getElements(Object inputElement)
        {
            return ((LinkedList<String>) inputElement).toArray();
        }

        public void dispose()
        {
        }

        public void inputChanged(Viewer viewer, Object oldInput, Object newInput)
        {
        }

    }

    class RelationLabelProvider implements ITableLabelProvider
    {

        public Image getColumnImage(Object element, int columnIndex)
        {
            return null;
        }

        public String getColumnText(Object element, int columnIndex)
        {
            Relation r = (Relation) element;
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

    class RelationModifier implements ICellModifier
    {

        private Viewer viewer;

        public RelationModifier(Viewer v)
        {
            viewer = v;
        }

        public boolean canModify(Object element, String property)
        {
            return property.equals(properties[1]);
        }

        public Object getValue(Object element, String property)
        {
            Relation r = (Relation) element;
            if (property.equals(properties[0])) return r.getNameAndSchema();
            else if (property.equals(properties[1])) return r.getValueString();

            return null;
        }

        public void modify(Object element, String property, Object value)
        {
            if (element instanceof Item) element = ((Item) element).getData();

            Relation r = (Relation) element;
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

                if (values.size() == 0)
                {
                    // Add default values.
                    values.add("0");
                    values.add("0");
                }

                r.setValues(values);
            }

            viewer.refresh();
        }

    }

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
