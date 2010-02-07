package org.dbtoaster.cumulus.slicer;

import org.dbtoaster.cumulus.net.TException;

public interface PrimarySlicerNodeIFace extends SlicerNode.SlicerNodeIFace
{
    void bootstrap() throws TException;
}