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
import java.util.HashMap;

import lupos.cloud.pig.BagInformation;

/**
 * Diese Klasse verweist auf alle vorhandenen Filterausdrücke. Die Filter werden
 * möglichst früh ausgeführt. Dazu wird für jeden Filter überprüft ob alle
 * relevanten Variablen sich in der jeweiligen Bag befinden um den Filter
 * auszuführen.
 */
public class PigFilterExectuer implements IPigOperator {

	/** Zwischenergebnisse. */
	private ArrayList<BagInformation> intermediateBags;

	/** Liste der Filter-Operationen. */
	HashMap<BagInformation, ArrayList<PigFilterOperator>> bagToFilterList = null;

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
		this.bagToFilterList = new HashMap<BagInformation, ArrayList<PigFilterOperator>>();
		this.intermediateBags = intermediateBags;
		ArrayList<BagInformation> toRemove = new ArrayList<BagInformation>();
		ArrayList<BagInformation> toAdd = new ArrayList<BagInformation>();

		for (PigFilterOperator filter : filterOps) {
			this.checkIfFilterPossible(filter);
		}
		if (bagToFilterList.size() > 0) {
			for (BagInformation curBag : bagToFilterList.keySet()) {
				if (debug) {
					int i = 0;
					result.append("-- Filter: ");
					for (PigFilterOperator filter : bagToFilterList
							.get(curBag)) {
						if (i > 0) {
							result.append(" AND ");
						}
						result.append(filter.getPigFilter());
						i++;
					}
					result.append("\n");
				}

				BagInformation newBag = new BagInformation("INTERMEDIATE_BAG_"
						+ BagInformation.idCounter);

				result.append(newBag.getName() + " = FILTER "
						+ curBag.getName() + " BY ");
				int i = 0;
				for (PigFilterOperator filter : bagToFilterList.get(curBag)) {
					if (i > 0) {
						result.append(" AND ");
					}
					String pigFilterVarReplaced = filter.getPigFilter();
					for (String var : filter.getVariables()) {
						pigFilterVarReplaced = pigFilterVarReplaced.replace(
								var,
								getPigNameForVariable("?" + var,
										curBag.getBagElements()));
					}

					result.append(pigFilterVarReplaced);

					newBag.addAppliedFilters(filter);
					curBag.addAppliedFilters(filter);

					i++;
				}

				result.append(";\n");

				if (debug) {
					result.append("\n");
				}

				newBag.setPatternId(BagInformation.idCounter);
				newBag.setJoinElements(curBag.getBagElements());
				newBag.addAppliedFilters(curBag.getAppliedFilters());
				newBag.addBitVectors(curBag.getBitVectors());

				toRemove.add(curBag);

				// toRemove.remove(curBag);
				toAdd.add(newBag);

				BagInformation.idCounter++;

				for (BagInformation item : toRemove) {
					intermediateBags.remove(item);
				}

				for (BagInformation item : toAdd) {
					intermediateBags.add(item);
				}
			}
		}
		return result.toString();
	}

	/**
	 * Check if filter possible.
	 * 
	 * @param filter
	 *            the filter
	 */
	private void checkIfFilterPossible(PigFilterOperator filter) {
		// Wenn alle Variablen in einer Menge vorkommen, kann der Filter
		// angewandt weden
		for (BagInformation curBag : intermediateBags) {

			// Wenn die Menge nicht schon einmal gefiltert wurde
			if (!curBag.filterApplied(filter)) {
				boolean varNotFound = false;
				for (String var : filter.getVariables()) {
					if (!curBag.getBagElements().contains("?" + var)) {
						varNotFound = true;
					}
				}
				if (!varNotFound) {
					addFilterToBag(curBag, filter);

				}
			}
		}

	}

	/**
	 * Gets the pig index for variable.
	 * 
	 * @param name
	 *            the name
	 * @param sparqlVariableList
	 *            the sparql variable list
	 * @return the pig name for variable
	 */
	private String getPigNameForVariable(String name,
			ArrayList<String> sparqlVariableList) {
		for (int i = 0; i < sparqlVariableList.size(); i++) {
			if (sparqlVariableList.get(i).equals(name)) {
				return "$" + i;
			}
		}
		return null; // Fall sollte nicht vorkommen
	}

	/**
	 * Adds the filter to bag.
	 * 
	 * @param toAdd
	 *            the to add
	 * @param filter
	 *            the filter
	 */
	private void addFilterToBag(BagInformation toAdd, PigFilterOperator filter) {
		ArrayList<PigFilterOperator> list = bagToFilterList.get(toAdd);
		if (list == null) {
			list = new ArrayList<PigFilterOperator>();
			list.add(filter);
			this.bagToFilterList.put(toAdd, list);
		} else {
			list.add(filter);
		}

	}

}
