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
package lupos.cloud.operator;

import java.util.ArrayList;

import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.BasicIndexScan;

/**
 * Enthält EIN IndexScan-Operator und die darauf folgenden Operationen.
 */
public class IndexScanContainer extends BasicOperator {
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5612770902234058839L;
	
	/** id counter . */
	private static int idCounter = 0;
	
	/** The id. */
	private int id;
	
	/** The index scan. */
	private BasicIndexScan indexScan;
	
	/** Folgeoperationen. */
	private ArrayList<BasicOperator> ops = new ArrayList<BasicOperator>();
	
	/** True, wenn eine Operation nicht unterstützt wird. */
	private boolean oneOperatorWasNotSupported;

	/**
	 * Instantiates a new index scan container.
	 *
	 * @param indexScan the index scan
	 */
	public IndexScanContainer(BasicIndexScan indexScan) {
		this.indexScan = indexScan;
		this.id = idCounter;
		idCounter++;
	}

	/**
	 * Fügt einen Operator hinzu.
	 *
	 * @param node the node
	 */
	public void addOperator(BasicOperator node) {
		this.ops.add(node);
	}

	/* (non-Javadoc)
	 * @see lupos.engine.operators.BasicOperator#toString()
	 */
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- IndexScanContainer (" + indexScan.getTriplePattern().size() + ")  --- \n");
		for (BasicOperator op : ops) {
			result.append(op.getClass().getSimpleName() + "\n");
		}

		return result.toString();
	}
	
	/**
	 * Gibt den IndexScan-Operator zurück.
	 *
	 * @return the index scan
	 */
	public BasicIndexScan getIndexScan() {
		return indexScan;
	}
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public int getId() {
		return id;
	}
	
	/**
	 * Gets the operators.
	 *
	 * @return the operators
	 */
	public ArrayList<BasicOperator> getOperators() {
		return ops;
	}
	
	/**
	 * One operator was not supported.
	 *
	 * @param b the b
	 */
	public void oneOperatorWasNotSupported(boolean b) {
		this.oneOperatorWasNotSupported = b;
	}
	
	/**
	 * Checks if is one operator was not supported.
	 *
	 * @return true, if is one operator was not supported
	 */
	public boolean isOneOperatorWasNotSupported() {
		return oneOperatorWasNotSupported;
	}

}
