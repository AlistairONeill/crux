package crux.api

import crux.api.Crux as JCrux
import crux.api.NodeConfigurator

object CruxKt {
    fun startNode(f: NodeConfigurator.() -> Unit) =
        JCrux.startNode(NodeConfigurator().apply(f).modules)
}