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
import java.util.LinkedList;
import java.util.TreeMap;

import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.multiinput.MultiInputOperator;
import lupos.engine.operators.singleinput.Projection;

/**
 * Enthält EINEN MultiIndexScan-Operator und die Folgeoprationen.
 */
public class MultiIndexScanContainer extends BasicOperator {

	/** Id counter. */
	private static int idCounter = 0;

	/** Folgeoperation. */
	private ArrayList<BasicOperator> ops = new ArrayList<BasicOperator>();

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5612770902234058839L;

	/** The multi index scan list. */
	TreeMap<Integer, LinkedList<BasicOperator>> multiIndexScanList = new TreeMap<Integer, LinkedList<BasicOperator>>();

	/** Mapping tree. */
	TreeMap<Integer, MultiInputOperator> mappingTree = new TreeMap<Integer, MultiInputOperator>();

	/** True, wenn eine Operation nicht unterstützt wird. */
	private boolean oneOperatorWasNotSupported;

	/**
	 * Fügt einen Container hinzu.
	 * 
	 * @param type
	 *            the type
	 * @param ops
	 *            the ops
	 */
	public void addSubContainer(MultiInputOperator type,
			LinkedList<BasicOperator> ops) {
		multiIndexScanList.put(idCounter, ops);
		mappingTree.put(idCounter, type);
		idCounter++;
	}

	/**
	 * Gets the container list.
	 * 
	 * @return the container list
	 */
	public TreeMap<Integer, LinkedList<BasicOperator>> getContainerList() {
		return multiIndexScanList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lupos.engine.operators.BasicOperator#toString()
	 */
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("--- MultiIndexScanContainer ---\n");
		for (BasicOperator op : ops) {
			if (op instanceof Projection) {
				result.append(((Projection) op).toString() + "\n");
			} else {
				result.append(op.getClass().getSimpleName() + "\n");
			}
		}

		return result.toString();
	}

	/**
	 * Gets the mapping tree.
	 * 
	 * @return the mapping tree
	 */
	public TreeMap<Integer, MultiInputOperator> getMappingTree() {
		return mappingTree;
	}

	/**
	 * Adds the operator to all childs.
	 * 
	 * @param op
	 *            the op
	 */
	public void addOperatorToAllChilds(BasicOperator op) {
		for (LinkedList<BasicOperator> curList : multiIndexScanList.values()) {
			for (BasicOperator node : curList) {
				if (node instanceof IndexScanContainer) {
					((IndexScanContainer) node).addOperator(op);
				} else {
					((MultiIndexScanContainer) node).addOperatorToAllChilds(op);
				}
			}
		}
	}

	/**
	 * Adds the operator.
	 * 
	 * @param op
	 *            the op
	 */
	public void addOperator(BasicOperator op) {
		this.ops.add(op);
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
	 * @param b
	 *            the b
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
