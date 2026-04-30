\ Example ILP: product planning

Maximize
  obj: 40 x1 + 30 x2

Subject To
  labor: 2 x1 + x2 <= 100
  material: x1 + 2 x2 <= 80
  demand_x1: x1 <= 40
  demand_x2: x2 <= 50

Bounds
  x1 >= 0
  x2 >= 0

General
  x1 x2

End