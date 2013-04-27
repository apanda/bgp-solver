package main
import (
        "os"
        "fmt"
        "strconv"
        "flag"
         topology "github.com/apanda/smpc/topology"
        )
type Topology struct {
    Nodes int
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
    NextHop map[int64] map[int64] int64
}
func (topo *Topology) InitTopology (nodes int) {
    topo.AdjacencyMatrix = make(map[int64] []int64, nodes)
    topo.NodeToPortMap = make(map[int64] map[int64] int64, nodes)
    topo.PortToNodeMap = make(map[int64] map[int64] int64, nodes)
    topo.IndicesLink = make(map[int64] []int64, nodes)
    topo.IndicesNode = make(map[int64] []int64, nodes)
    topo.Exports = make(map[int64] [][]int64, nodes)
    topo.NextHop = make(map[int64] map[int64] int64, nodes)
    topo.Nodes = nodes
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
        topo.NextHop[node] = make(map[int64] int64, nodes)
        for node2 := range topo.AdjacencyMatrix {
            if node2 == node {
                topo.NextHop[node][node2] = node2
            } else {
                topo.NextHop[node][node2] = 0
            }
        }
    }
    return topo
}

func (topo *Topology) GetCurrentNextHop (node int64, dest int64) (int64){
    // Go through neighbors in preference order
    for nbrIdx := range topo.IndicesNode[node] {
        nbr := topo.IndicesNode[node][nbrIdx]
        nbrLink := topo.NodeToPortMap[nbr][node]
        nbrNhop := topo.NextHop[dest][nbr]
        nbrNhopLink := topo.NodeToPortMap[nbr][nbrNhop]
        export := topo.Exports[nbr][nbrLink][nbrNhopLink]
        if export * nbrNhop != 0 {
            return nbr
        }
    }
    return 0
}

func (topo *Topology) ComputeNextHops (dest int64) {
    converged := false
    for !converged {
        nhopTable := make(map[int64] int64, topo.Nodes)
        converged = true
        for node := range topo.AdjacencyMatrix {
            nhopTable[node] = topo.GetCurrentNextHop(node, dest)
            converged = converged && (nhopTable[node] == topo.NextHop[dest][node])
        }
        topo.NextHop[dest] = nhopTable
    }
}

func main() {
    topoFile := flag.String("topology", "", "Topology (json file) to use")
    flag.Parse()
    if *topoFile == "" || topoFile == nil {
        flag.Usage()
        os.Exit(1)        
    }
    fmt.Printf("Reading JSON\n")
    topo := JsonTopoToTopo(topology.ParseJsonTopology(topoFile))
    fmt.Printf("Done reading JSON\n")
    for dest := range topo.AdjacencyMatrix {
        fmt.Printf("Computing for dest %d\n", dest)
        topo.ComputeNextHops(dest)
    }
    for i := range topo.NextHop {
        fmt.Printf("%d: ", i)
        for j := range topo.NextHop[i] {
            fmt.Printf("%d ", topo.NextHop[i][j])
        }
        fmt.Printf("\n")
    }
}

