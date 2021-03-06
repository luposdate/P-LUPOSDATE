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

/**
 * Union Operator.
 */
public class PigUnionOperator implements IPigOperator {
	
	/** Zwischenergebnisse. */
	private BagInformation newBag;
	
	/** The multi inputist. */
	private ArrayList<BagInformation> multiInputist;

	/**
	 * Instantiates a new pig union operator.
	 *
	 * @param newJoin the new join
	 * @param multiInputist the multi inputist
	 */
	public PigUnionOperator(BagInformation newJoin,
			ArrayList<BagInformation> multiInputist) {
		this.newBag = newJoin;
		this.multiInputist = multiInputist;
	}

	/* (non-Javadoc)
	 * @see lupos.cloud.pig.operator.IPigOperator#buildQuery(java.util.ArrayList, boolean, java.util.ArrayList)
	 */
	public String buildQuery(ArrayList<BagInformation> intermediateBags,
			boolean debug, ArrayList<PigFilterOperator> filterOps) {
		StringBuilder result = new StringBuilder();
		if (debug) {
			result.append("-- UNION:\n");
		}
		result.append(newBag.getName() + " = UNION ");
		;
		for (int i = 0; i < multiInputist.size(); i++) {
			if (i == 0) {
				result.append(multiInputist.get(i).getName());
			} else {
				result.append(", " + multiInputist.get(i).getName());
			}
		}
		result.append(";\n\n");
		newBag.setJoinElements(multiInputist.get(0).getBagElements());

		for (BagInformation bag : multiInputist) {
			for (String elem : bag.getBagElements()) {
				if (!multiInputist.get(0).getBagElements().contains(elem)) {
					newBag.addBagElements(elem);
					newBag.addOptionalElements(elem);
				}
			}
		}

		return result.toString();
	}
}
