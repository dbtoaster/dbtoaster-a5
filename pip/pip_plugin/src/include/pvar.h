//////////////////////////////////////////////////////////////////////////
// pvar.h
// 
// Operations for manipulating, and querying pip_vars. 
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_PVAR_H
#define PIP_PVAR_H

int pip_var_id_parse(char *str, pip_var_id *id);
int pip_var_parse(char *str, pip_var **pvar);
int pip_var_parse_params(char *str, pip_var *pvar);

int pip_var_id_sprint(char *str, int len, pip_var_id *pvar);
int pip_var_sprint(char *str, int len, pip_var *pvar);

bool   pip_var_id_eq(pip_var_id *a, pip_var_id *b);
bool   pip_var_eq(pip_var *a, pip_var *b);
int pip_variable_sort(pip_var *a, pip_var *b);

float8 pip_var_gen(pip_var *var);

//The following functions evaluate the PDF/CDF/inverse CDF of the variable
//in question.  If the requested function is not implemented by a particular 
//distribution, these functions will return NAN (test for this with math.h:isnan())
//Convenience macros useful for checking whether a variable has one of these
//functions defined are present in include/dist.h
float8 pip_var_pdf(pip_var *var, float8 point);
float8 pip_var_cdf(pip_var *var, float8 point);
float8 pip_var_icdf(pip_var *var, float8 point);

//generate a randomly distributed value from the given seed, hashing the name into the seed
//This function is deterministic for the given seed/variable combination
// - the variable name is hashed into the seed value, so two different variable
//   will generate different values, even if they have the same parameters. 
float8 pip_var_gen_w_name_and_seed(pip_var *var, int64 seed);

//generate a randomly distributed value from the given seed
//This function is deterministic for the given seed
// - There will very likely be correlations between different variables.  For almost
//   all purposes, pip_var_gen_w_name_and_seed() is more appropriate.
float8 pip_var_gen_wseed(pip_var *var, int64 seed);

//generate a "bounded" randomly distributed value.
//This function operates off of the distribution's iCDF; consequently the provided bounds
//are restricted to the range [0.0, 1.0], where 0.0 corresponds to the variable's lower bound
//and 1.0 corresponds to the variable's upper bound.  Note that this function will return 
//NAN if any of its parameters are in error, or if the provided function does not have an iCDF.
float8 pip_var_gen_w_range(pip_var *var, float8 low, float8 high);


#endif