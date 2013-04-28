package main
import (
        "os"
        "bufio"
        "fmt"
        "strconv"
        "flag"
        "sort"
        "runtime"
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

func (topo *Topology) LinkFailEffect (node0 int64, node1 int64) (map[int64] int) {
    node0Result := make(map[int64] int, topo.Nodes)
    for dest := range topo.NextHop {
        if dest == node0 {
            continue
        }
        if topo.NextHop[dest][node0] != node1 { //Case 1
             //fmt.Printf("Failed %d %d, path to %d unaffected\n", node0, node1, dest)
            node0Result[dest] = 1
        } else {
            //fmt.Printf("Computing next hops\n")
            nhops := topo.ComputeNextHopsWithFail(topo.NextHop[dest], node0, node1)
            nhop := nhops[node0] // Correct next hop for failure
            //fmt.Printf("Done computing next hop\n")
            if nhop == 0 || topo.NextHop[dest][nhop] == node0 { //Case 3
                // fmt.Printf("Failed %d %d, path to %d instaloops (%d)\n", node0, node1, dest, nhop)
                //fmt.Printf("Case 3 (instant loop), fail %d %d, dest %d nhop %d\n", node0, node1, dest, nhop)
                node0Result[dest] = 3
            } else {
                // Simulate path 
                current := nhop
                pathLength := 0
                prev := int64(0)
                //fmt.Printf("Simulating (%d %d failed, dest %d)\n", node0, node1, dest)
                visited := make(map[int64] bool, topo.Nodes)
                path := make([]int64, topo.Nodes)
                path[pathLength] = node0
                pathLength += 1
                path[pathLength] = nhop
                pathLength += 1
                visited[nhop] = true
                for ((current != dest) && (current != node0)) {
                    prev = current
                    current = topo.NextHop[dest][current]
                  //  fmt.Printf("Simulation step node %d dest %d current %d\n", node0, dest, current)
                    if prev == current {
                        fmt.Printf("Weird loop, dying\n")
                        os.Exit(1)
                    }
                    
                    path[pathLength] = current
                    pathLength += 1
                    if visited[current] {
                        fmt.Printf("Hmm loop\nDest: %d\nPath: ", dest)
                        for idx := 0; idx < pathLength; idx++ {
                            fmt.Printf("%d ", path[idx])
                        }
                        fmt.Printf("\n")
                        os.Exit(1)
                    }
                    visited[current] = true
                }
                //fmt.Printf("Path length of %d\n", pathLength)
                if (current == dest) { // Case 2
                    // fmt.Printf("Failed %d %d, path to %d changes but gets to dest in %d\n", node0, node1, dest, pathLength)
                    node0Result[dest] = 2
                } else { // Case 4
                    // fmt.Printf("Failed %d %d, path to %d changes and loops\n", node0, node1, dest)
                    node0Result[dest] = 4
                }
            }
        }
    }
    return node0Result
}

func (topo *Topology) GetCurrentNextHopWithFail (node int64, nhop map[int64] int64, disallow int64) (int64) {
    // Go through neighbors in preference order
    for nbrIdx := range topo.IndicesNode[node] {
        nbr := topo.IndicesNode[node][nbrIdx]
        if nbr == disallow {
            continue
        }
        nbrLink := topo.NodeToPortMap[nbr][node]
        nbrNhop := nhop[nbr]
        nbrNhopLink := topo.NodeToPortMap[nbr][nbrNhop]
        export := topo.Exports[nbr][nbrLink][nbrNhopLink]
        if export * nbrNhop != 0 {
            return nbr
        }
    }
    return 0
}

func (topo *Topology) ComputeNextHopsWithFail (nhop map[int64] int64, src int64, disallow int64) (map[int64] int64) {
    converged := false
    steps := 0
    for !converged {
        nhopTable := make(map[int64] int64, topo.Nodes)
        converged = true
        steps++
        for node := range topo.AdjacencyMatrix {
            if node == src {
                nhopTable[node] = topo.GetCurrentNextHopWithFail(node,  nhop, disallow)
            } else {
                nhopTable[node] = topo.GetCurrentNextHop(node, nhop)
            }
            converged = converged && (nhopTable[node] == nhop[node])
        }
        nhop = nhopTable
        if steps >= 10 && steps % 10 == 0 {
            fmt.Printf("No converges in %d steps\n", steps)
        }
        if steps >= 12 {
            break
        }
    }
    fmt.Printf("Convergence after failurer took %d steps\n", steps)
    return nhop
}

func (topo *Topology) GetCurrentNextHop (node int64, nhop map[int64] int64) (int64){
    // Go through neighbors in preference order
    for nbrIdx := range topo.IndicesNode[node] {
        nbr := topo.IndicesNode[node][nbrIdx]
        nbrLink := topo.NodeToPortMap[nbr][node]
        nbrNhop := nhop[nbr]
        nbrNhopLink := topo.NodeToPortMap[nbr][nbrNhop]
        export := topo.Exports[nbr][nbrLink][nbrNhopLink]
        if export * nbrNhop != 0 {
            return nbr
        }
    }
    return 0
}

func (topo *Topology) ComputeNextHopsInternal (nhop map[int64] int64) (map[int64] int64) {
    converged := false
    nhopTable := make(map[int64] int64, topo.Nodes)
    for !converged {
        converged = true
        for node := range topo.AdjacencyMatrix {
            nhopTable[node] = topo.GetCurrentNextHop(node, nhop)
            converged = converged && (nhopTable[node] == nhop[node])
        }
        nhop, nhopTable = nhopTable, nhop
    }
    return nhop
}


func (topo *Topology) PrintNextHop () {
    for i := range topo.NextHop {
        fmt.Printf("%d: ", i)
        for j := range topo.NextHop[i] {
            fmt.Printf("%d:%d ", j, topo.NextHop[i][j])
        }
        fmt.Printf("\n")
    }
}

type int64arr []int64
func (a int64arr) Len() int { 
    return len(a) 
}

func (a int64arr) Swap(i, j int) { 
    a[i], a[j] = a[j], a[i] 
}
func (a int64arr) Less(i, j int) bool {
    return a[i] < a[j] 
}


func main() {
    topoFile := flag.String("topology", "", "Topology (json file) to use")
    outFile := flag.String("out", "", "Output file")
    flag.Parse()
    if *topoFile == "" || topoFile == nil || outFile == nil || *outFile == "" {
        flag.Usage()
        os.Exit(1)        
    }
    fmt.Printf("Num CPU = %d\n", runtime.NumCPU())
    runtime.GOMAXPROCS(runtime.NumCPU())
    fmt.Printf("Reading JSON\n")
    topo := JsonTopoToTopo(topology.ParseJsonTopology(topoFile))
    fmt.Printf("Done reading JSON\n")
    fmt.Printf("Starting next hop computation\n")
    count := 0
    ch := make(map[int64] chan map[int64] int64, len(topo.AdjacencyMatrix))
    for dest := range topo.AdjacencyMatrix {
        ch[dest] = make(chan map[int64] int64, 1)
        go func(d int64, nhop map[int64] int64) {
            ch[d] <- topo.ComputeNextHopsInternal (nhop)
        }(dest, topo.NextHop[dest])
    }
    for idx := range ch {
        topo.NextHop[idx] =  <- ch[idx]
        count ++
        fmt.Printf("Done %d/%d\n", count, len(topo.AdjacencyMatrix))
    }
    //topo.PrintNextHop()
    of, err := os.Create(*outFile)
    defer of.Close()
    bufOf := bufio.NewWriter(of)
    defer bufOf.Flush()
    if err != nil {
        fmt.Printf("Error opening file %v\n", err)
        os.Exit(1)
    }
    dests := int64arr(make([]int64, topo.Nodes))
    count = 0
    for node := range topo.AdjacencyMatrix {
        dests[count] = node
        count++
    }
    sort.Sort(dests)
    bufOf.WriteString("Node0 Node1 ")
    for didx := range dests {
        bufOf.WriteString(fmt.Sprintf("%d ", dests[didx]))
    }
    bufOf.WriteString("\n")
    bufOf.Flush()
    count = 0
    fmt.Printf("Starting the main course\n")
    for node0 := range topo.AdjacencyMatrix {
        chFail := make(map[int64] chan map[int64] int, len(topo.AdjacencyMatrix[node0]))
        for idx := range topo.AdjacencyMatrix[node0] {
            node1 := topo.AdjacencyMatrix[node0][idx]
            if node1 == node0 {
                continue
            }
            chFail[node1] = make(chan map[int64] int, 1)
            go func(n0 int64, n1 int64) {
                fmt.Printf("Starting computation %d %d\n", n0, n1)
                out := topo.LinkFailEffect(node0, node1)
                fmt.Printf("Computed failure of %d %d\n", n0, n1)
                chFail[n1] <- out
            } (node0, node1)
        }
        for idx := range topo.AdjacencyMatrix[node0] {
            node1 := topo.AdjacencyMatrix[node0][idx]
            if node1 == node0 {
                continue
            }
            fmt.Printf("Waiting for %d %d\n", node0, node1)
            out := <- chFail[node1]
            bufOf.WriteString(fmt.Sprintf("%d %d", node0, node1))
            for didx := range dests {
                bufOf.WriteString(fmt.Sprintf("%d ", out[dests[didx]]))
            }
            bufOf.WriteString("\n")
        }
        count++
        fmt.Printf("Done with node %d/%d\n", count, topo.Nodes)
        bufOf.Flush()
    }
}
