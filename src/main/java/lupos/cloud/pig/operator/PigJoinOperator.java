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
import lupos.datastructures.items.Variable;
import lupos.engine.operators.multiinput.join.Join;

/**
 * Join Operator
 */
public class PigJoinOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	private BagInformation newBag;

	/** Menge der zu joinenden Bags. */
	private ArrayList<BagInformation> multiInputist;

	/** Luposdate Join. */
	private Join join;

	/**
	 * Instantiates a new pig join operator.
	 * 
	 * @param newBag
	 *            the new join
	 * @param multiInputist
	 *            the multi inputist
	 * @param join
	 *            the join
	 */
	public PigJoinOperator(BagInformation newBag,
			ArrayList<BagInformation> multiInputist, Join join) {
		this.newBag = newBag;
		this.multiInputist = multiInputist;
		this.join = join;
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
		if (debug) {
			result.append(" -- JOIN:\n");
		}
		result.append(newBag.getName() + " = JOIN ");
		for (int i = 0; i < multiInputist.size(); i++) {
			if (i > 0) {
				result.append(", ");
			}

			result.append(multiInputist.get(i).getName() + " BY ");

			ArrayList<Variable> joinList = new ArrayList<Variable>(
					join.getIntersectionVariables());

			if (joinList.size() == 0) {
				throw new RuntimeException(
						"Es sind keine Intersection Variablen für die Operation Join vorhanden -> Abbruch!");
			}

			if (joinList.size() == 1) {
				result.append("$"
						+ multiInputist.get(i).getBagElements()
								.indexOf("?" + joinList.get(0).getName()));
			} else {
				int j = 0;
				result.append("(");
				for (Variable var : joinList) {
					if (j > 0) {
						result.append(",");
					}
					result.append("$"
							+ multiInputist.get(j).getBagElements()
									.indexOf("?" + var.getName()));
					j++;
				}
				result.append(")");
			}
		}
		result.append(";\n\n");

		ArrayList<String> joinElements = new ArrayList<String>();
		for (Variable var : join.getIntersectionVariables()) {
			joinElements.add("?" + var.getName());
		}

		boolean firstBag = true;
		for (BagInformation bag : multiInputist) {
			if (firstBag) {
				newBag.setJoinElements(bag.getBagElements());
				firstBag = false;
			} else {
				for (String var : bag.getBagElements()) {
					newBag.addBagElements(var);
				}
			}
		}
		return result.toString();
	}
}
