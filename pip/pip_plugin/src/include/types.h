/*
 * PIP Datatypes
 *
 *
 */

#ifndef PIP_TYPES_H
#define PIP_TYPES_H

typedef struct pip_var_id {
  int64 variable;
  int64 group;
} pip_var_id;

typedef struct pip_var {
  char vl_len_[4]; //internal length parameter; do not touch!
  pip_var_id vid;
  int64 subscript;
  char group_state[0];
} pip_var;

typedef struct pip_eqn {
  char vl_len_[4];
  char data[0]; // pip_eqn_component (eqn.h) <- flattened tree
} pip_eqn;

typedef struct pip_exp {
  char vl_len_[4];
  enum pip_exp_type {
    PIP_EXP_EQN, PIP_EXP_FIX
  } type;
  union {
    pip_eqn eqn;
    float8 fix;
  } val;
} pip_exp;

typedef struct pip_atom { //represents ptr_left > ptr_right
  char vl_len_[4]; //internal length parameter; do not touch!
  int16 ptr_left, ptr_right;
  char data[0]; // pip_eqn_component (eqn.h) <- flattened tree
} pip_atom;

typedef struct pip_atomset {
  char vl_len_[4]; //internal length parameter; do not touch!
  float8 probability; //precomputed probability of the atoms being true.
  int cachestate;
  int count;
  char data[0]; //set of pip_atom, use vl_len_ to figure out deltas.
}

typedef pip_presample_tree pip_sample_generator; //281 bytes

typedef struct pip_sample_mapping {
  pip_var_id var;  //since we've already instantiated, we can get away with just the ID
  float8 val[0]; // float8 * sample_cnt
} pip_sample_mapping; 

typedef struct pip_sample_set {
  char vl_len_[4]; //internal length parameter; do not touch!
  int32 sample_cnt;
  int32 var_cnt;
  int64 ssid;
  int64 seed;
  char data[0]; // pip_sample_mapping * var_cnt
} pip_sample_set;

typedef struct pip_conf_tally_group {
  int64 ssid;
  int32 sample_cnt;
} pip_conf_tally_group;

typedef struct pip_conf_tally {
  char vl_len_[4]; //internal length parameter; do not touch!
  int64 group_cnt;
  int32 sample_cnt;
  char data[0]; // pip_conf_tally_group * group_cnt || int32 * sample_cnt
} pip_conf_tally;

typedef struct pip_value_bundle {
  char vl_len_[4]; //internal length parameter; do not touch!
  int64 seed;
  int32 worldcount;
  char data[0]; //pip_var || float8 * worldcount
} pip_value_bundle;

typedef struct pip_world_presence {
  char vl_len_[4]; //internal length parameter; do not touch!
  int32 worldcount;
  unsigned char data[0];//bit array
} pip_world_presence;

#endif
