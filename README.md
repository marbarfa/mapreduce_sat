MapReduce implementation for kSAT problem.
=============


The algorithm first will be developed to run in hadoop 1.0.4 but
would be nice to afterwards compare the algorithm with spark.

The general idea of the algorithm is described below:


The first implementation is a simple approach to evaluate its performance and then maybe improve the algorithm adding some heuristics, etc.

1) Fix i variables
=> split the problem in 2^i subproblems
(eg: fix variables x0 and x1 => split the problem in 4 subproblems: x0x1={(0,0),(0,1),(1,0),(1,1)}).

2) Fix d variables => try to fix d variables. 

3) Some Mappers maybe found variables to fix and others maybe have found a false clause because of the initial fix
(e.g. xox1 = (0,1) makes clause Ci false).
Save this value to prune the search tree.

4) Reducer -> restart algorithm with new split if solution was not found. Use learnt variables to start subproblem in valid branches.
Each mapper will split its subproblem in another 2^i subproblems.
*Usually will be more problems than mappers.

Simple example:
                                          fix  x_o,x_1

0,0                         0,1                       1,0                           1,1 --> Map
|                            |                         |                             |
|unsat!                      |                         |                             |
                             |                         |                             |
------------------------------------------------------------------------------------------> Reduce
split in 2^i:            | | | |                    | | | |                       | | | |

The search tree would be something like the following, for this simple example:

                                      |
                   |           |             |            |
                 ||||        ||||          ||||         ||||

