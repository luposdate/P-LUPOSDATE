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
package lupos.cloud.operator.format;

import java.util.ArrayList;
import java.util.LinkedList;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.pig.BagInformation;
import lupos.cloud.pig.PigQuery;
import lupos.cloud.pig.operator.PigJoinOperator;
import lupos.cloud.pig.operator.PigOptionalOperator;
import lupos.cloud.pig.operator.PigUnionOperator;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.multiinput.Union;
import lupos.engine.operators.multiinput.join.Join;
import lupos.engine.operators.multiinput.optional.Optional;

/**
 * Formatierer für den MultiIndexScan-Container.
 */
public class MultiIndexScanFormatter implements IOperatorFormatter {
	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * luposdate.operators.formatter.OperatorFormatter#serialize(lupos.engine
	 * .operators.BasicOperator, int)
	 */
	public PigQuery serialize(final BasicOperator operator, final PigQuery pigQuery) {
		final MultiIndexScanContainer multiIndexScanContainer = (MultiIndexScanContainer) operator;
		this.joinMultiIndexScans(multiIndexScanContainer, pigQuery);
		return pigQuery;
	}

	/**
	 * Join multi index scans.
	 *
	 * @param container
	 *            the container
	 * @param pigQuery
	 *            the pig query
	 * @return the join information
	 */
	public BagInformation joinMultiIndexScans(
			final MultiIndexScanContainer container, final PigQuery pigQuery) {
		BagInformation newJoin = null;
		for (final Integer id : container.getContainerList().keySet()) {
			final LinkedList<BasicOperator> curList = container.getContainerList()
					.get(id);
			final ArrayList<BagInformation> multiInputist = new ArrayList<BagInformation>();
			for (final BasicOperator op : curList) {
				if (op instanceof IndexScanContainer) {
					new IndexScanCointainerFormatter().serialize(op, pigQuery);
					multiInputist.add(pigQuery.getLastAddedBag());
				} else if (op instanceof MultiIndexScanContainer) {
					final MultiIndexScanContainer c = (MultiIndexScanContainer) op;
					multiInputist.add(this.joinMultiIndexScans(c, pigQuery));

				}
			}

			newJoin = new BagInformation();
			boolean isJoin = false;
			if (container.getMappingTree().get(id) instanceof Union) {
				pigQuery.buildAndAppendQuery(new PigUnionOperator(newJoin,
						multiInputist));
			} else if (container.getMappingTree().get(id) instanceof Join) {
				isJoin = true;
				pigQuery.buildAndAppendQuery(new PigJoinOperator(newJoin,
						multiInputist, (Join) container.getMappingTree()
								.get(id)));
			} else if (container.getMappingTree().get(id) instanceof Optional) {
				pigQuery.buildAndAppendQuery(new PigOptionalOperator(newJoin,
						multiInputist, (Optional) container.getMappingTree()
								.get(id)));
			} else {
				throw new RuntimeException(
						"Multi input operator not found. Add it! -> "
								+ container.getMappingTree().get(id).getClass());
			}

			int i = 0;
			for (final BagInformation toRemove : multiInputist) {
				newJoin.addAppliedFilters(toRemove.getAppliedFilters());
				pigQuery.removeIntermediateBags(toRemove);

				if (isJoin || i == 0) {
					for (final String var : toRemove.getBitVectors().keySet()) {
						newJoin.addBitvector(var, toRemove.getBitVector(var));
					}
				} else {
					for (final String var : toRemove.getBitVectors().keySet()) {
						newJoin.mergeBitVecor(var, toRemove.getBitVector(var));
					}
				}
				i++;
			}

			newJoin.mergeOptionalVariables(multiInputist);

			pigQuery.addIntermediateBags(newJoin);
		}

		pigQuery.addAndExecuteOperation(container.getOperators());
		return pigQuery.getLastAddedBag();
	}

}
