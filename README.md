# Calculate Overlap For Gb files

This program is designed to take CDR data in the form of fakecall.txt.
The output is a unique edgelist with weights, overlap, weighted overlap, and any other bowtie functions.
This program has an eye toward speed, and allows the user to specify the amount of RAM used.  Only base Python required.
The speed of the algorithm is proportional to n*(log(n) + n/nRAM),
      where n is the number of unique edges and nRAM is the number of these stored in RAM.
Gains in speed are therefore directly related to RAM usage, which has diminishing returns if overburdened.
Assuming a 2 GHz processor and 500Mb available RAM, I believe this takes somewhere between 4 hours and 1 day.
(Give me a break, as far as I can tell it takes 8 minutes just to read through the Gb CDR file!!!)

The overall approach is to perform an external mergesort the edges.
This allows you to aggregate identical edges, because they are next to each other!
Then, scan the edgelist and store a list of all the nodes.
For batches of nodes with size of your choosing, store the edgelist for the nodes AND their neighbors in RAM.
Use these to calculate overlap, weighted overlap, and other bowtie functions in order of the edgelist.
