/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;

/**
 * Maintains a list of weak references to all clones. On close ensure all clones are closed as well.
 * Per {@link IndexInput} cloned IndexInputs can be closed with the parent.
 */
public class RefTrackedOnDemandBlockSnapshotIndexInput extends OnDemandBlockSnapshotIndexInput {

    private final ArrayList<WeakReference<IndexInput>> cloneRefs;

    public RefTrackedOnDemandBlockSnapshotIndexInput(OnDemandBlockSnapshotIndexInput parentObject, ArrayList<WeakReference<IndexInput>> parentRefList) {
        super(parentObject);
        this.cloneRefs = parentRefList;
    }

    public RefTrackedOnDemandBlockSnapshotIndexInput(BlobStoreIndexShardSnapshot.FileInfo fileInfo, FSDirectory directory, TransferManager transferManager) {
        super(fileInfo, directory, transferManager);
        this.cloneRefs = new ArrayList<>();
    }

    @Override
    public RefTrackedOnDemandBlockSnapshotIndexInput clone() {
        RefTrackedOnDemandBlockSnapshotIndexInput retClone = new RefTrackedOnDemandBlockSnapshotIndexInput(super.clone(), cloneRefs);
        cloneRefs.add(new WeakReference<>(retClone));
        return retClone;
    }

    @Override
    public void close() throws IOException {
        if (!isClone) {
            for (WeakReference<IndexInput> ref : cloneRefs) {
                IndexInput input = ref.get();
                if (input != null) {
                    System.out.println("CLOSING AN UNCLOSED REF: " + input);

                    // input.close();
                }

                System.out.println("NO CLOSE - REF ALREADY NULL: " + input);

                // cloneRefs.remove(ref); // For now don't remove for debugging
            }
        }
        super.close();
    }
}
