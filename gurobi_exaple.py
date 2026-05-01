import gurobipy as gp
model = gp.read("inputs/ilp_input1.lp")
model.optimize()