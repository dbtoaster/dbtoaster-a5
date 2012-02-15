-------------------- SOURCES --------------------
CREATE STREAM R(R_A int, R_B int)
  FROM FILE '../../experiments/data/big_r.dat' LINE DELIMITED
  CSV(fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE STREAM S(S_B int, S_C int)
  FROM FILE '../../experiments/data/big_s.dat' LINE DELIMITED
  CSV(fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE STREAM T(T_C int, T_D int)
  FROM FILE '../../experiments/data/big_t.dat' LINE DELIMITED
  CSV(fields := ',', schema := 'int,int', eventtype := 'insert');

--------------------- MAPS ----------------------
DECLARE MAP AtimesD_mR3(int)[][AtimesD_mRR_B] := 
    AggSum([AtimesD_mRR_B],((S(AtimesD_mRR_B, S_C) * T(S_C, T_D) * T_D)));

DECLARE MAP AtimesD_mS2_mR3(int)[][AtimesD_mSS_C] := 
    AggSum([AtimesD_mSS_C],((T(AtimesD_mSS_C, T_D) * T_D)));

DECLARE MAP AtimesD_mS2(int)[][AtimesD_mSS_B, AtimesD_mSS_C] := 
    AggSum([AtimesD_mSS_B, AtimesD_mSS_C],((R(R_A, AtimesD_mSS_B) * T(AtimesD_mSS_C, T_D) * R_A * T_D)));

DECLARE MAP AtimesD_mT3_mR2(int)[][AtimesD_mT3_mRR_B, AtimesD_mTT_C] := 
    S(AtimesD_mT3_mRR_B, AtimesD_mTT_C);

DECLARE MAP AtimesD_mT3_mS2(int)[][AtimesD_mT3_mSS_B] := 
    AggSum([AtimesD_mT3_mSS_B],((R(R_A, AtimesD_mT3_mSS_B) * R_A)));

DECLARE MAP AtimesD_mT3(int)[][AtimesD_mTT_C] := 
    AggSum([AtimesD_mTT_C],((R(R_A, R_B) * S(R_B, AtimesD_mTT_C) * R_A)));

DECLARE MAP AtimesD(int)[][] := 
    AggSum([],((R(R_A, R_B) * S(R_B, S_C) * T(S_C, T_D) * R_A * T_D)));

-------------------- QUERIES --------------------
DECLARE QUERY AtimesD := AtimesD(int)[][];

------------------- TRIGGERS --------------------
ON + R(R_A, R_B) {
   AtimesD(int)[][] += (R_A * AtimesD_mR3(int)[][R_B]);
   AtimesD_mT3(int)[][AtimesD_mTT_C] += (AtimesD_mT3_mR2(int)[][R_B, AtimesD_mTT_C] * R_A);
   AtimesD_mT3_mS2(int)[][R_B] += R_A;
   AtimesD_mS2(int)[][R_B, AtimesD_mSS_C] += (R_A * AtimesD_mS2_mR3(int)[][AtimesD_mSS_C]);
}

ON - R(R_A, R_B) {
   AtimesD(int)[][] += (-1 * R_A * AtimesD_mR3(int)[][R_B]);
   AtimesD_mT3(int)[][AtimesD_mTT_C] += (-1 * AtimesD_mT3_mR2(int)[][R_B, AtimesD_mTT_C] * R_A);
   AtimesD_mT3_mS2(int)[][R_B] += (-1 * R_A);
   AtimesD_mS2(int)[][R_B, AtimesD_mSS_C] += (-1 * R_A * AtimesD_mS2_mR3(int)[][AtimesD_mSS_C]);
}

ON + S(S_B, S_C) {
   AtimesD(int)[][] += AtimesD_mS2(int)[][S_B, S_C];
   AtimesD_mT3(int)[][S_C] += AtimesD_mT3_mS2(int)[][S_B];
   AtimesD_mT3_mR2(int)[][S_B, S_C] += 1;
   AtimesD_mR3(int)[][S_B] += AtimesD_mS2_mR3(int)[][S_C];
}

ON - S(S_B, S_C) {
   AtimesD(int)[][] += (-1 * AtimesD_mS2(int)[][S_B, S_C]);
   AtimesD_mT3(int)[][S_C] += (-1 * AtimesD_mT3_mS2(int)[][S_B]);
   AtimesD_mT3_mR2(int)[][S_B, S_C] += -1;
   AtimesD_mR3(int)[][S_B] += (-1 * AtimesD_mS2_mR3(int)[][S_C]);
}

ON + T(T_C, T_D) {
   AtimesD(int)[][] += (T_D * AtimesD_mT3(int)[][T_C]);
   AtimesD_mS2(int)[][AtimesD_mSS_B, T_C] += (T_D * AtimesD_mT3_mS2(int)[][AtimesD_mSS_B]);
   AtimesD_mS2_mR3(int)[][T_C] += T_D;
   AtimesD_mR3(int)[][AtimesD_mRR_B] += (T_D * AtimesD_mT3_mR2(int)[][AtimesD_mRR_B, T_C]);
}

ON - T(T_C, T_D) {
   AtimesD(int)[][] += (-1 * T_D * AtimesD_mT3(int)[][T_C]);
   AtimesD_mS2(int)[][AtimesD_mSS_B, T_C] += (-1 * T_D * AtimesD_mT3_mS2(int)[][AtimesD_mSS_B]);
   AtimesD_mS2_mR3(int)[][T_C] += (-1 * T_D);
   AtimesD_mR3(int)[][AtimesD_mRR_B] += (-1 * T_D * AtimesD_mT3_mR2(int)[][AtimesD_mRR_B, T_C]);
}
