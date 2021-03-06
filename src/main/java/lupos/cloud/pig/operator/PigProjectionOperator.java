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
import java.util.HashSet;

import lupos.cloud.pig.BagInformation;
import lupos.datastructures.items.Variable;

/**
 * Projektsions Operator. Die Projektion wird so früh wie Möglich ausgeführt.
 * Bevor Variablen gedropt werden, wird überprüft ob die jeweilige Variable ggf.
 * noch für Filter o.Ä. gebraucht wird.
 */
public class PigProjectionOperator implements IPigOperator {

	/** Variablen. */
	HashSet<String> projectionVariables;

	/** Zwischenergebnisse. */
	private ArrayList<BagInformation> intermediateJoins;

	/** The debug. */
	private boolean debug;

	/** Filter Operationen. */
	ArrayList<PigFilterOperator> filterOps;

	/**
	 * Instantiates a new pig projection operator.
	 *
	 * @param projection
	 *            the projection
	 */
	public PigProjectionOperator(final HashSet<Variable> projection) {
		this.projectionVariables = new HashSet<String>();
		for (final Variable varToAdd : projection) {
			this.projectionVariables.add(varToAdd.toString());
		}
	}

	/**
	 * Adds the projection varibles.
	 *
	 * @param variables
	 *            the variables
	 */
	public void addProjectionVaribles(final HashSet<String> variables) {
		for (final String varToAdd : variables) {
			this.projectionVariables.add(varToAdd);
		}
	}

	/**
	 * Gets the projection variables.
	 *
	 * @return the projection variables
	 */
	public HashSet<String> getProjectionVariables() {
		return this.projectionVariables;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * lupos.cloud.pig.operator.IPigOperator#buildQuery(java.util.ArrayList,
	 * boolean, java.util.ArrayList)
	 */
	public String buildQuery(final ArrayList<BagInformation> intermediateBags,
			final boolean debug, final ArrayList<PigFilterOperator> filterOps) {
		this.intermediateJoins = intermediateBags;
		this.filterOps = filterOps;
		this.debug = debug;
		return this.checkIfProjectionPossible();
	}

	/**
	 * Check if projection possible.
	 *
	 * @return the string
	 */
	private String checkIfProjectionPossible() {
		final StringBuilder result = new StringBuilder();
		if (this.projectionVariables.size() != 0) {
			final HashMap<BagInformation, HashSet<String>> varJoinMap = this.getValidProjectionVariables();

			for (final BagInformation curBag : varJoinMap.keySet()) {

				// Projektion ist nicht notwendig
				if (this.joinListAndProjectionListAreEquals(
						curBag.getBagElements(), varJoinMap.get(curBag))) {
					continue;
				} else {
					if (this.debug) {
						result.append("-- Projection: "
								+ varJoinMap.get(curBag).toString()
										.replace("[", "").replace("]", "")
								+ "\n");
					}

					final BagInformation newBag = new BagInformation(
							"INTERMEDIATE_BAG_" + BagInformation.idCounter);

					result.append(newBag.getName() + " = FOREACH "
							+ curBag.getName() + " GENERATE ");

					int i = 0;
					for (final String var : varJoinMap.get(curBag)) {
						newBag.getBagElements().add(var);
						result.append("$"
								+ curBag.getBagElements().indexOf(var));
						if (i + 1 < varJoinMap.get(curBag).size()) {
							result.append(", ");
						} else {
							result.append(";\n");
						}
						i++;
					}

					if (this.debug) {
						result.append("\n");
					}

					newBag.setPatternId(BagInformation.idCounter);
					newBag.setJoinElements(new ArrayList<String>(varJoinMap
							.get(curBag)));

					newBag.mergeOptionalVariables(curBag);
					newBag.addAppliedFilters(curBag.getAppliedFilters());
					newBag.addBitVectors(curBag.getBitVectors());

					this.intermediateJoins.remove(curBag);
					this.intermediateJoins.add(newBag);
					BagInformation.idCounter++;
				}
			}
		}
		return result.toString();
	}

	/**
	 * Gets the valid projection variables.
	 *
	 * @return the valid projection variables
	 */
	private HashMap<BagInformation, HashSet<String>> getValidProjectionVariables() {
		final HashMap<BagInformation, HashSet<String>> varJoinMap = new HashMap<BagInformation, HashSet<String>>();
		for (final String projectionVar : this.projectionVariables) {
			int varCounter = 0;
			BagInformation projectionJoin = null;
			for (final BagInformation item : this.intermediateJoins) {
				if (item.getBagElements().contains(projectionVar)) {
					varCounter++;
					projectionJoin = item;
				}
			}

			if (varCounter == 1) {
				// prüfe ob gedropte variablen noch gebraucht werden,
				// aonsonsten nicht droppen
				final HashSet<String> dropNotAllowedList = new HashSet<String>();
				for (final String dropCandidateVariable : projectionJoin
						.getBagElements()) {
					// Variable wird noch für Joins mit anderen Bags gebraucht?
					for (final BagInformation otherJoin : this.intermediateJoins) {
						if (!otherJoin.equals(projectionJoin)) {
							if (otherJoin.getBagElements().contains(
									dropCandidateVariable)) {
								dropNotAllowedList.add(dropCandidateVariable);
							}

						}
					}

					// Variable wird noch für einen Filter gebraucht?
					for (final PigFilterOperator pigFilter : this.filterOps) {
						for (final String filterVar : pigFilter.getVariables()) {
							if (dropCandidateVariable.equals("?" + filterVar)) {
								// Wenn eine Filtervariable gedropt werden soll
								// überprüfe ob Filter schon angewendet wurde,
								// wenn ja kann sie gedroppt weden
								for (final BagInformation join : this.intermediateJoins) {
									if (join.getBagElements().contains(
											"?" + filterVar)
											&& !join.getAppliedFilters()
													.contains(pigFilter)) {
										dropNotAllowedList
												.add(dropCandidateVariable);
									}

								}

							}
						}
					}

				}

				// Wende die Projetion nur an, wenn sich die Liste
				// verkleinert
				if (dropNotAllowedList.size() + 1 < projectionJoin
						.getBagElements().size()) {

					final HashSet<String> varList = varJoinMap.get(projectionJoin);
					if (varList != null) {
						varList.add(projectionVar);
					} else {
						final HashSet<String> newList = new HashSet<String>();
						newList.add(projectionVar);
						varJoinMap.put(projectionJoin, newList);
					}

					for (final String noDrop : dropNotAllowedList) {
						varJoinMap.get(projectionJoin).add(noDrop);
					}
				}
			}
		}
		return varJoinMap;
	}

	/**
	 * Join list and projection list are equals.
	 *
	 * @param joinElements
	 *            the join elements
	 * @param compareList
	 *            the compare list
	 * @return true, if successful
	 */
	private boolean joinListAndProjectionListAreEquals(
			final ArrayList<String> joinElements, final HashSet<String> compareList) {
		if (joinElements.size() != compareList.size()) {
			return false;
		}
		for (final String elem : joinElements) {
			if (!compareList.contains(elem)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Replace variable in projection.
	 *
	 * @param addBinding
	 *            the add binding
	 */
	public void replaceVariableInProjection(final HashMap<String, String> addBinding) {
		if (addBinding != null) {
			for (final String oldVar : addBinding.keySet()) {
				final boolean removed = this.projectionVariables.remove(oldVar);
				if (removed) {
					this.projectionVariables.add(addBinding.get(oldVar));
				}
			}
		}
	}
}
