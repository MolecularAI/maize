Glossary
========
Brief definitions of terms used by the *maize* documentation.

.. glossary::
   
   component
      A component of a graph, or the graph itself. It is also the base class to :class:`~maize.core.node.Node` and :class:`~maize.core.graph.Graph`.

   node
      A :term:`component` of a graph that contains no further nodes, i.e. it is atomic, or from a tree point-of-view, a leaf node. See :class:`~maize.core.node.Node`.

   graph
      A :term:`component` that contains multiple nodes. If it's the root-level graph it is referred to as a :term:`workflow`. 

   subgraph
      A :term:`graph` that is itself a :term:`component` of a graph, i.e. not a top-level :term:`workflow`.

   workflow
      The top / root-level :term:`graph` containing all subgraphs or nodes. See :class:`~maize.core.workflow.Workflow`.

   interface
      An interface to a :term:`component`, i.e. either some kind of :term:`parameter` or :term:`port`. See the :class:`~maize.core.interface.Interface` base class.

   port
      A port can be either an :term:`input` or :term:`output` and represents the primary :term:`interface` for communication between components. See :class:`~maize.core.interface.Port`.

   parameter
      A parameter allows setting up a :term:`component` before execution, using either files (:class:`~maize.core.interface.FileParameter`) or arbitrary data (:class:`~maize.core.interface.Parameter`).

   input
      Any kind of :term:`component` input, can be set to a :term:`channel`. See :class:`~maize.core.interface.Input` and :class:`~maize.core.interface.MultiInput`.

   output
      Any kind of :term:`component` output, can be set to a :term:`channel`. See :class:`~maize.core.interface.Output` and :class:`~maize.core.interface.MultiOutput`.

   channel
      Any kind of unidirectional communication channel between components. They have no information about their connection partners and can only be connected to a single :term:`input` and :term:`output`. See :class:`~maize.core.channels.DataChannel` and :class:`~maize.core.channels.FileChannel`.