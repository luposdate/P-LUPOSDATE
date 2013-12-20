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

import java.util.Collection;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.pig.PigQuery;
import lupos.cloud.pig.SinglePigQuery;
import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigIndexScanOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigOrderByOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.AddBinding;
import lupos.engine.operators.singleinput.AddBindingFromOtherVar;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.engine.operators.singleinput.modifiers.Limit;
import lupos.engine.operators.singleinput.modifiers.SortLimit;
import lupos.engine.operators.singleinput.modifiers.distinct.Distinct;
import lupos.engine.operators.singleinput.sort.Sort;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * Implements the formatter for the index scan operator.
 */
public class IndexScanCointainerFormatter implements IOperatorFormatter {
	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * luposdate.operators.formatter.OperatorFormatter#serialize(lupos.engine
	 * .operators.BasicOperator, int)
	 */
	public PigQuery serialize(final BasicOperator operator, final PigQuery pigQuery) {
		final SinglePigQuery singlePigQuery = new SinglePigQuery();
		final IndexScanContainer indexScan = (IndexScanContainer) operator;
		final Collection<TriplePattern> tp = indexScan.getIndexScan()
				.getTriplePattern();
		final PigIndexScanOperator pigIndexScan = new PigIndexScanOperator(tp);
		singlePigQuery.setIndexScanOperator(pigIndexScan);
		singlePigQuery.buildAndAppendQuery(pigIndexScan);
		for (final BasicOperator op : indexScan.getOperators()) {
			if (op instanceof Filter) {
				singlePigQuery.addContainerFilter(new PigFilterOperator(
						(Filter) op));
			} else if (op instanceof Projection) {
				singlePigQuery.addProjection(new PigProjectionOperator(
						((Projection) op).getProjectedVariables()));
			} else if (op instanceof Distinct) {
				singlePigQuery.setDistinctOperator(new PigDistinctOperator());
			} else if (op instanceof Limit || op instanceof SortLimit) {
				if (op instanceof Limit) {
					singlePigQuery.setLimitOperator(new PigLimitOperator(
							((Limit) op).getLimit()));
				} else if (op instanceof SortLimit) {
					singlePigQuery.setLimitOperator(new PigLimitOperator(
							// Workaround, es gibt fuer SortLimit kein "getLimit()"
							Integer.parseInt(((SortLimit) op).toString()
									.substring(
											((SortLimit) op).toString()
													.indexOf("SortLimit ") + "SortLimit ".length()))));
				}
			} else if (op instanceof Sort) {
				singlePigQuery.setOrderbyOperator(new PigOrderByOperator(
						((Sort) op)));
			} else if (op instanceof AddBindingFromOtherVar) {
				// muss an sich nicht behandelt werden, da im
				// IndexScan/TripelMustern die Variablen bereits durch LUPOSDATE
				// ersetzew werden, jedoch nicht in der Projektion!
				final AddBindingFromOtherVar addVar = (AddBindingFromOtherVar) op;
				singlePigQuery.replaceVariableInProjection("?"
						+ addVar.getVar().getName(), "?"
						+ addVar.getOtherVar().getName());

			} else if (op instanceof Result || op instanceof Root
					|| op instanceof AddBinding) {
				// ignore
			} else {
				throw new RuntimeException(
						"Something is wrong here. Forgot case? Class: "
								+ op.getClass());
			}
		}

		pigQuery.addAndPrceedSinglePigQuery(singlePigQuery);
		return pigQuery;
	}
}
