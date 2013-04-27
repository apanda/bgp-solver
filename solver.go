package main
import (
        //"fmt"
        "strconv"
        "flag"
         topology "github.com/apanda/smpc/topology"
        )
type Topology struct {
    // Map from node to other connected nodes
    // Node -> link -> node (links are ordered i.e. 0, 1, 2, 3...)
    AdjacencyMatrix map[int64] []int64
    // Node -> node -> link
    NodeToPortMap map[int64] map[int64] int64
    PortToNodeMap map[int64] map[int64] int64
    // Node -> int -> int -> bool
    // Node -> rank -> index -> bool (says whether for a node, rank x is link y)
    // For node is rank 0 index foo
    IndicesLink map[int64] []int64
    IndicesNode map[int64] []int64
    Exports map[int64] [][]int64
    NextHop map[int64] int64
}
func (topo *Topology) InitTopology (nodes int) {
    topo.AdjacencyMatrix = make(map[int64] []int64, nodes)
    topo.NodeToPortMap = make(map[int64] map[int64] int64, nodes)
    topo.PortToNodeMap = make(map[int64] map[int64] int64, nodes)
    topo.IndicesLink = make(map[int64] []int64, nodes)
    topo.IndicesNode = make(map[int64] []int64, nodes)
    topo.Exports = make(map[int64] [][]int64, nodes)
    topo.NextHop = make(map[int64] int64, nodes)

    for i := int64(0); i < int64(nodes); i++ {
        topo.PortToNodeMap[i + 1] = make(map[int64] int64, nodes)
        topo.NodeToPortMap[i + 1] = make(map[int64] int64, nodes)
    }
}

func JsonTopoToTopo(json *topology.JsonTopology) (*Topology) {
    topo := &Topology{}
    nodes := len(json.AdjacencyMatrix)
    topo.InitTopology (nodes)
    for node := range json.AdjacencyMatrix {
        nint32, _  := strconv.Atoi(node)
        nint :=  int64(nint32)
        topo.AdjacencyMatrix[nint] = json.AdjacencyMatrix[node]
    }
    for node := range json.PortToNodeMap {
        nint32, _  := strconv.Atoi(node)
        nint :=  int64(nint32)
        for link := range json.PortToNodeMap[node] {
            topo.PortToNodeMap[nint][int64(link)] = json.PortToNodeMap[node][link]
        }
    }

    for node := range json.NodeToPortMap {
        nint32, _  := strconv.Atoi(node)
        nint := int64(nint32)
        for onode := range json.NodeToPortMap[node] {
            onint32, _ := strconv.Atoi(onode)
            onodeint := int64(onint32)
            topo.NodeToPortMap[nint][onodeint] = json.NodeToPortMap[node][onode]
        }
    }

    for node := range json.ExportTables {
        nint32, _  := strconv.Atoi(node)
        nint := int64(nint32)
        topo.Exports[nint] = json.ExportTables[node]
    }
    
    for node := range json.IndicesLink {
        nint32, _  := strconv.Atoi(node)
        nint := int64(nint32)
        topo.IndicesLink[nint] = json.IndicesLink[node]
        topo.IndicesNode[nint] = json.IndicesNode[node]
    }
    for node := range topo.AdjacencyMatrix {
        topo.NextHop[node] = 0
    }
    return topo
}
func main() {
    topoFile := flag.String("topology", "", "Topology (json file) to use")
    flag.Parse()
    if *topoFile == "" || topoFile == nil {
        flag.Usage()
    }
    topo := JsonTopoToTopo(topology.ParseJsonTopology(topoFile))
    _ = topo
}
