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

import lupos.cloud.operator.format.CloudSubgraphContainerFormatter;
import lupos.cloud.pig.PigQuery;
import lupos.datastructures.bindings.BindingsFactory;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.operators.RootChild;
import lupos.engine.operators.index.Dataset;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.messages.BindingsFactoryMessage;
import lupos.engine.operators.messages.BoundVariablesMessage;
import lupos.engine.operators.messages.Message;
import lupos.rdf.Prefix;

/**
 * This container contains all operators that shall be send to a the cloud for
 * execution.
 *
 */
public class CloudSubgraphContainer extends RootChild {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/**
	 * The root node of the sub graph.
	 */
	private final Root rootNodeOfSubGraph;

	private BindingsFactory bindingsFactory;

	/**
	 * the executor to submit a subgraph and retrieve its query result...
	 */
	private final ICloudSubgraphExecutor cloudSubgraphExecutor;

	/**
	 * Instantiates a new sub graph container.
	 *
	 * @param rootNodeOfSubGraph
	 *            the root node of sub graph
	 * @param subgraphExecutor
	 *            the subgraph executor
	 */
	public CloudSubgraphContainer(final Root rootNodeOfSubGraph,
			final ICloudSubgraphExecutor subgraphExecutor) {
		this.rootNodeOfSubGraph = rootNodeOfSubGraph;
		this.cloudSubgraphExecutor = subgraphExecutor;
	}

	@Override
	public Message preProcessMessage(final BindingsFactoryMessage msg){
		this.bindingsFactory = msg.getBindingsFactory();
		return msg;
	}

	/**
	 * Gets called when the operator is to be executed. When called this method
	 * sends the sub graph to the responsible nodes for execution and waits for
	 * the result to return.
	 *
	 * @param dataset
	 *            the data set
	 *
	 * @return the result of the query execution
	 */
	@Override
	public QueryResult process(final Dataset dataset) {
		final CloudSubgraphContainerFormatter pigParser = new CloudSubgraphContainerFormatter();
		final PigQuery pigQuery = pigParser.serialize(this.rootNodeOfSubGraph,
				new PigQuery());
		final QueryResult result = this.cloudSubgraphExecutor.evaluate(pigQuery, this.bindingsFactory);
		// result.materialize();
		return result;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see lupos.engine.operators.BasicOperator#toString(lupos.rdf.Prefix)
	 */
	@Override
	public String toString(final Prefix prefixInstance) {
		return this.toString();
	}

	/**
	 * Gets the root of subgraph.
	 *
	 * @return the root of subgraph
	 */
	public Root getRootOfSubgraph() {
		return this.rootNodeOfSubGraph;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * lupos.engine.operators.BasicOperator#preProcessMessage(lupos.engine.operators
	 * .messages.BoundVariablesMessage)
	 */
	@Override
	public Message preProcessMessage(final BoundVariablesMessage msg) {
		final BoundVariablesMessage newMsg = new BoundVariablesMessage(msg);
		newMsg.setVariables(this.getUnionVariables());
		return newMsg;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see lupos.engine.operators.BasicOperator#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder result = new StringBuilder();
		result.append("--- Cloud SubgraphContainer ---\n");
		return result.toString();
	}
}
