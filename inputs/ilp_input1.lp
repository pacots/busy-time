\ Busy Time ILP
\ Capacity = 2
\ Time slots are 1..8, where slot t means [t, t+1)

Minimize
  obj: y1 + y2 + y3 + y4 + y5 + y6 + y7 + y8

Subject To
  \ Each job receives its required processing time
  job_A: x_A_1 + x_A_2 + x_A_3 = 2
  job_B: x_B_1 + x_B_2 + x_B_3 + x_B_4 + x_B_5 = 3
  job_C: x_C_2 + x_C_3 + x_C_4 = 1
  job_D: x_D_3 + x_D_4 + x_D_5 + x_D_6 = 3
  job_E: x_E_5 + x_E_6 + x_E_7 = 2
  job_F: x_F_6 + x_F_7 + x_F_8 = 2
  job_G: x_G_7 + x_G_8 = 1

  \ Capacity constraints: at most 2 jobs can run in a busy slot
  cap_1: x_A_1 + x_B_1 - 2 y1 <= 0
  cap_2: x_A_2 + x_B_2 + x_C_2 - 2 y2 <= 0
  cap_3: x_A_3 + x_B_3 + x_C_3 + x_D_3 - 2 y3 <= 0
  cap_4: x_B_4 + x_C_4 + x_D_4 - 2 y4 <= 0
  cap_5: x_B_5 + x_D_5 + x_E_5 - 2 y5 <= 0
  cap_6: x_D_6 + x_E_6 + x_F_6 - 2 y6 <= 0
  cap_7: x_E_7 + x_F_7 + x_G_7 - 2 y7 <= 0
  cap_8: x_F_8 + x_G_8 - 2 y8 <= 0

Binary
  y1 y2 y3 y4 y5 y6 y7 y8
  x_A_1 x_A_2 x_A_3
  x_B_1 x_B_2 x_B_3 x_B_4 x_B_5
  x_C_2 x_C_3 x_C_4
  x_D_3 x_D_4 x_D_5 x_D_6
  x_E_5 x_E_6 x_E_7
  x_F_6 x_F_7 x_F_8
  x_G_7 x_G_8

End