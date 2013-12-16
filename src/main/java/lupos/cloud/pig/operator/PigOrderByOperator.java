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
package lupos.cloud.pig.operator;

import java.util.ArrayList;

import lupos.cloud.pig.BagInformation;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.singleinput.sort.Sort;

/**
 * Order By Operator.
 */
public class PigOrderByOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	private ArrayList<BagInformation> intermediateJoins;

	/** Luposdate Operator. */
	private Sort orderByLuposOperation = null;

	/**
	 * Instantiates a new pig order by operator.
	 * 
	 * @param sort
	 *            the sort
	 */
	public PigOrderByOperator(Sort sort) {
		this.orderByLuposOperation = sort;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lupos.cloud.pig.operator.IPigOperator#buildQuery(java.util.ArrayList,
	 * boolean, java.util.ArrayList)
	 */
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		this.intermediateJoins = intermediateBags;

		ArrayList<Variable> list = new ArrayList<Variable>(
				orderByLuposOperation.getSortCriterium());

		if (debug) {
			result.append("-- ORDER BY ?" + list.get(0).getName() + "\n");
		}

		BagInformation curBag = intermediateJoins.get(0);
		BagInformation newBag = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		// TODO: add ASC/DESC choice
//		ComparatorBindings comparator = orderByLuposOperation.getComparator();

		result.append(newBag.getName() + " = ORDER " + curBag.getName()
				+ " BY" + " $"
				+ curBag.getItemPos("?" + list.get(0).getName()));

		if (CloudManagement.PARALLEL_REDUCE_OPERATIONS > 1) {
			result.append(" PARALLEL "
					+ CloudManagement.PARALLEL_REDUCE_OPERATIONS);
		}

		result.append(";\n");

		if (debug) {
			result.append("\n");
		}

		newBag.setPatternId(BagInformation.idCounter);
		newBag.setJoinElements(curBag.getBagElements());
		newBag.addAppliedFilters(curBag.getAppliedFilters());
		newBag.mergeOptionalVariables(curBag);
		newBag.addBitVectors(curBag.getBitVectors());

		intermediateJoins.remove(curBag);
		intermediateJoins.add(newBag);
		BagInformation.idCounter++;
		return result.toString();
	}
}
