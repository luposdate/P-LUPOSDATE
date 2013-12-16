/**
 * Copyright (c) 2013, Institute of Information Systems (Sven Groppe, Thomas Kiencke and contributors of P-LUPOSDATE), University of Luebeck
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 	- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * 	  disclaimer.
 * 	- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * 	  following disclaimer in the documentation and/or other materials provided with the distribution.
 * 	- Neither the name of the University of Luebeck nor the names of its contributors may be used to endorse or promote
 * 	  products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package lupos.cloud.storage;

import java.util.ArrayList;
import java.util.Collection;

import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.hbase.HBaseTriple;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Triple;
import lupos.datastructures.queryresult.QueryResult;
import lupos.distributed.storage.nodistributionstrategy.BlockUpdatesStorage;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * This class contains the storage layer for our distributed SPARQL cloud query
 * evaluator. This class handles the communication with the SPARQL cloud during
 * data manipulation and distributed querying.
 */
public class Storage_Cloud extends BlockUpdatesStorage {

	/**
	 * for the communication with the cloud.
	 */
	protected final CloudManagement cloudManagement;

	/** this flag is true if data has been inserted, otherwise it is false. */
	protected boolean insertedData = false;

	/** The storage instance. */
	public static Storage_Cloud storageInstance = null;

	/**
	 * Gets the single instance of Storage_Cloud.
	 *
	 * @return single instance of Storage_Cloud
	 */
	public static Storage_Cloud getInstance() {
		if (storageInstance == null) {
			storageInstance = new Storage_Cloud();
		}
		return storageInstance;
	}

	/**
	 * Constructor: The clooud management is initialized.
	 */
	public Storage_Cloud() {
		this.cloudManagement = new CloudManagement();
	}

	/**
	 * Gets the cloud management.
	 *
	 * @return the cloud management
	 */
	public CloudManagement getCloudManagement() {
		return cloudManagement;
	}

	/* (non-Javadoc)
	 * @see lupos.distributed.storage.nodistributionstrategy.BlockUpdatesStorage#blockInsert()
	 */
	@Override
	public void blockInsert() {
		this.cloudManagement.submitHBaseTripleToDatabase(buildInputHBaseTriple(this.toBeAdded));
		this.insertedData = true;
	}

	/* (non-Javadoc)
	 * @see lupos.distributed.storage.nodistributionstrategy.BlockUpdatesStorage#containsTripleAfterAdding(lupos.datastructures.items.Triple)
	 */
	@Override
	public boolean containsTripleAfterAdding(final Triple triple) {
		// relevant?
		return true;
	}

	/* (non-Javadoc)
	 * @see lupos.distributed.storage.nodistributionstrategy.BlockUpdatesStorage#removeAfterAdding(lupos.datastructures.items.Triple)
	 */
	@Override
	public void removeAfterAdding(final Triple triple) {
		this.cloudManagement
				.deleteHBaseTripleFromDatabase(HBaseDistributionStrategy
						.getTableInstance().generateIndecesTriple(triple));
	}

	/* (non-Javadoc)
	 * @see lupos.distributed.storage.nodistributionstrategy.BlockUpdatesStorage#evaluateTriplePatternAfterAdding(lupos.engine.operators.tripleoperator.TriplePattern)
	 */
	@Override
	public QueryResult evaluateTriplePatternAfterAdding(
			final TriplePattern triplePattern) {
		// relevant?
		return null;
	}

	/* (non-Javadoc)
	 * @see lupos.distributed.storage.nodistributionstrategy.BlockUpdatesStorage#endImportData()
	 */
	@Override
	public void endImportData() {
		if (!this.toBeAdded.isEmpty()) {
			super.endImportData();
		}
		if (this.insertedData) {
			this.insertedData = false;
		}
	}

	/**
	 * Helper Function to create HBaseTriple.
	 *
	 * @param toBeAdded the to be added
	 * @return the collection
	 */
	public static Collection<HBaseTriple> buildInputHBaseTriple(
			final Collection<Triple> toBeAdded) {
		ArrayList<HBaseTriple> hbaseTripleList = new ArrayList<HBaseTriple>();
		for (Triple triple : toBeAdded) {
			for (HBaseTriple ht : HBaseDistributionStrategy.getTableInstance()
					.generateIndecesTriple(triple))
				hbaseTripleList.add(ht);
		}
		return hbaseTripleList;
	}
}
