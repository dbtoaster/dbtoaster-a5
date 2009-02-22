//////////////////////////////////////////////////////////////////////////
// sample_set.h
// 
// Sample sets are used for two things
//  - Storing a compressed representation of variable-value mappings for
//    a fixed number of possible worlds.  The default representation of
//    a sample set includes only an integer seed that is combined with
//    the variable name and passed to the generator to produce a value
//    deterministically.  Specific variables may be singled out to be
//    defined with specific values.
//  - Storing a vector of values.  This is used when aggregating
//    histograms.
//
// All sample sets are assigned a unique identifier.  This is particularly
// relevant when generating confidence tallies in karp-luby; Each 
// sample set is matched with its entry in the confidence tally by this
// identifier.
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_SAMPLESET_H
#define PIP_SAMPLESET_H


pip_sample_set *pip_sample_set_create(int32 sample_count, int32 var_count);

// Predefined value manipulation functions
void        pip_sample_name_set      (pip_sample_set *set, int var, pip_var_id *name);
pip_var_id *pip_sample_name_get      (pip_sample_set *set, int var);
bool        pip_sample_name_is       (pip_sample_set *set, int var, pip_var_id *name);
void        pip_sample_val_set_by_id (pip_sample_set *set, int var, int sample, float8 val);
void        pip_sample_val_set       (pip_sample_set *set, pip_var_id *var, int sample, float8 val);
float8      pip_sample_val_get_by_id (pip_sample_set *set, int var, int sample);
float8      pip_sample_val_get       (pip_sample_set *set, pip_var_id *var, int sample);
int64       pip_sample_seed          (pip_sample_set *set, int sample);

// pip_sample_val_get will return NAN if the value has not been specifically defined
// for the sample set.  For general lookups (ie, to generate the value from the 
// sampleset's seed, use this function)
float8 pip_sample_var_val(pip_sample_set *set, int sample, pip_var *var);

// Vector sampleset manipulation functions
pip_sample_set *pip_sample_set_vector_max (pip_eqn *eqn, pip_sample_set *set, int clause_cnt, pip_atom **clause);
pip_sample_set *pip_sample_set_vector_sum (pip_eqn *eqn, pip_sample_set *set, int clause_cnt, pip_atom **clause);


#endif
