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

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.pig.PigQuery;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.Result;


/**
 * Formatierer für CloudSubGraphContainer.
 */
public class CloudSubgraphContainerFormatter implements IOperatorFormatter {

	/**
	 * Instantiates a new cloud subgraph container formatter.
	 */
	public CloudSubgraphContainerFormatter() {
	}

	/* (non-Javadoc)
	 * @see lupos.cloud.operator.format.IOperatorFormatter#serialize(lupos.engine.operators.BasicOperator, lupos.cloud.pig.PigQuery)
	 */
	public PigQuery serialize(final BasicOperator operator, final PigQuery pigLatin) {
		final PigQuery result = this.serializeNode(new OperatorIDTuple(operator, 0),
				pigLatin);
		pigLatin.finishQuery();
		return result;
	}

	/**
	 * Serialize node.
	 *
	 * @param node the node
	 * @param pigLatin the pig latin
	 * @return the pig query
	 */
	private PigQuery serializeNode(final OperatorIDTuple node, final PigQuery pigLatin) {

		PigQuery result = null;
		final BasicOperator op = node.getOperator();

		IOperatorFormatter serializer = null;
		if (op instanceof Root || op instanceof Result) {
			// do nothing
			result = pigLatin;
		} else if (op instanceof IndexScanContainer) {
			serializer = new IndexScanCointainerFormatter();
		} else if (op instanceof MultiIndexScanContainer) {
			 serializer = new MultiIndexScanFormatter();
		} else {
			throw new RuntimeException(
					"Something is wrong here. Forgot case? Class: "
							+ op.getClass());
		}

		if (serializer != null) {
			result = serializer.serialize(op, pigLatin);
		}

		for (final OperatorIDTuple successor : op.getSucceedingOperators()) {
			result = this.serializeNode(successor, result);
		}

		return result;
	}
}