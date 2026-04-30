import gurobipy as gp
from gurobipy import GRB

model = gp.read("inputs/ilp_input1.lp")
model.optimize()

if model.status == GRB.OPTIMAL:
    print("Objective:", model.objVal)
    for v in model.getVars():
        print(v.varName, v.x)