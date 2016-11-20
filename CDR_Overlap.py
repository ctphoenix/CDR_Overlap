##########################################################################################
##########################################################################################
############################# Calculate Overlap For Gb files #############################
##########################################################################################
##########################################################################################

batch_nrow = 1000000 # total number of rows per batch, for sorting.  I suggest no larger than 1 million, and should take 1 hour.
num_nodes  = 100000  # total number of nodes to find overlap for, using RAM to store its neighbors and their neighbors.
                     # Both of these suggestions should be profiled on a larger file, perhaps a tenth of the real dataset.


########### Define All Functions ###########

def mergeiter(*iterables, **kwargs):
    iterables = [iter(it) for it in iterables]
    iterables = {i: [next(it), i, it] for i, it in enumerate(iterables)}
    key = lambda item, key=kwargs['key']: key(item[0])
    while True:
        value, i, it = min(iterables.values(), key=key)
        yield value
        try:
            iterables[i][0] = next(it)
        except StopIteration:
            del iterables[i]
            if not iterables:
                raise

def line_sort(line):
    line = line.strip().split(" ")
    return int(line[0]) + float(line[1])/1e10

def line_to_edge_and_weight(line):
    line = line.strip().split(";")
    edge = (int(line[1]), int(line[2]))
    weight = float(line[4])
    return edge, weight

def write_memory(memory, batch):
    memory_sorted_edges =  sorted(memory.items())
    print("Bytes RAM In Use: "+str(sys.getsizeof(memory_sorted_edges)*2))
    reversed_memory_sorted_edges =  sorted({(i[1], i[0]): memory[i] for i in memory}.items())
    with open("batches/batch"+str(batch)+".txt", "w") as G:
        for i in memory_sorted_edges:
            G.write(str(i[0][0])+" "+str(i[0][1])+" "+str(i[1])+"\n")
    with open("batches/rev_batch"+str(batch)+".txt", "w") as G:
        for i in reversed_memory_sorted_edges:
            G.write(str(i[0][0])+" "+str(i[0][1])+" "+str(i[1])+"\n")

def overlap(node_dict, node1, node2):
    shared = len([1 for node in node_dict[node1] if node in node_dict[node2]])
    k1, k2 = len(node_dict[node1]), len(node_dict[node2])
    denom = (k1+k2-shared-2)
    if denom == 0:
        return 0
    else:
        return shared / denom

def weighted_overlap(node_dict, node1, node2):
    shared = sum([node_dict[node1][node] for node in node_dict[node1] if node in node_dict[node2]])
    s1, s2 = sum(node_dict[node1].values()), sum(node_dict[node2].values())
    denom = s1+s2-2*node_dict[node1][node2]
    if denom == 0:
        return 0
    else:
        return shared / denom

def write_overlap(nodes):
    nodes_and_neighbors = set(nodes[:])
    with open("Unique_Sorted_Edgelist.txt", "r") as F:
        for line in F:
            line = line.strip().split(" ")
            line_node = line[0]
            if line_node in nodes:
                nodes_and_neighbors.add(line[1])
    node_dict = {node: {} for node in nodes_and_neighbors}
    with open("Unique_Sorted_Edgelist.txt", "r") as F:
        for line in F:
            line = line.strip().split(" ")
            line_node = line[0]
            if line_node in nodes_and_neighbors:
                node_dict[line_node][line[1]] = float(line[2])
    print("Bytes RAM In Use: "+str(sys.getsizeof(node_dict)))
    with open("Unique_Sorted_Edgelist.txt", "r") as F:
        with open("Overlap_Edgelist.txt", "a") as G:
            for line in F:
                line = line.strip().split(" ")
                if line[0] in nodes:
                    o = overlap(node_dict, line[0],line[1])
                    w = weighted_overlap(node_dict, line[0],line[1])
                    G.write(line[0]+" "+line[1]+" "+str(round(float(line[2]),4))+" "+str(round(o,4))+" "+str(round(w,4))+"\n")


########### Execute the code ###########

import operator, time, os, sys
global_start = start = time.time()

print("Sorting edges.")
if not os.path.exists("batches"):
    os.makedirs("batches")
memory = {}
batch = 0
with open("fakecall.txt", "r") as F:
    for line in F:
        edge, weight = line_to_edge_and_weight(line)
        if edge in memory:
            memory[edge] += weight
        else:
            memory[edge] = weight
        if len(memory) >= batch_nrow:
            write_memory(memory, batch)
            memory = {}
            batch += 1
    write_memory(memory, batch)
files = [open("batches/"+filename) for filename in os.listdir("batches")]
with open("Sorted_Edgelist.txt", "w") as F:
    for line in mergeiter(*files, key=line_sort):
        F.write(line+"")
for file in files:
    file.close() # Why doesn't this close the files?
#os.remove("batches") # this won't work until the files are closed.

print("Finished in "+str(round(time.time()-start,2))+" seconds.\n")
start = time.time()
print("Aggregating weights of unique edges.")
with open("Sorted_Edgelist.txt", "r") as F:
    line = F.readline().strip().split(" ")
    current_line, current_weight = line[:2], 0
with open("Sorted_Edgelist.txt", "r") as F:
    with open("Unique_Sorted_Edgelist.txt", "w") as G:
        for line in F:
            line = line.strip().split(" ")
            if current_line == line[:2]:
                current_weight += float(line[2])
            else:
                G.write(" ".join(current_line)+" "+str(current_weight)+"\n")
                current_line = line[:2]
                current_weight = float(line[2])
        G.write(" ".join(current_line)+" "+str(round(current_weight,4))+"\n")
os.remove("Sorted_Edgelist.txt")

print("Finished in "+str(round(time.time()-start,2))+" seconds.\n")
start = time.time()
print("Determining list of nodes.")

nodes = set()
with open("Unique_Sorted_Edgelist.txt", "r") as F:
    for line in F:
        line = line.strip().split(" ")
        nodes.add(int(line[0]))
        nodes.add(int(line[1]))
nodes = sorted(list(nodes))
with open("Nodelist.txt", "w") as F:
    for node in nodes:
        F.write(str(node)+"\n")

print("Finished in "+str(round(time.time()-start,2))+" seconds.\n")
start = time.time()
print("Finding overlap by node batch.")
if os.path.exists("Overlap_Edgelist.txt"):
    os.remove("Overlap_Edgelist.txt")
nodes = []
with open("Nodelist.txt", "r") as N:
    for node_line in N:
        nodes.append(node_line.strip())
        if len(nodes) >= num_nodes:
            write_overlap(nodes)
            nodes = []
    write_overlap(nodes)

if os.path.exists("Nodelist.txt"):
    os.remove("Nodelist.txt")
if os.path.exists("Unique_Sorted_Edgelist.txt"):
    os.remove("Unique_Sorted_Edgelist.txt")

print("Finished in "+str(round(time.time()-start,2))+" seconds.\n")
print("Everything finished in "+str(round(time.time()-global_start,2))+" seconds.")













