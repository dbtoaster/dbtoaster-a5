//////////////////////////////////////////////////////////////////////////
// dist.h
// 
// Operations for managing, manipulating, and defining custom 
// distributions.  New distributions are defined by files in src/dist/;
// The macro DECLARE_PIP_DISTRIBUTION both creates a distribution 
// definition structure (that should be filled in), and indicates to the
// script gen_disttable.sh that the new distribution should be added to
// the distribution table.  At present, the addition of new distribution
// types does require a recompile.
//
// Also of note, several prng operations are provided that take a seed
// value inline; These operations should be used in lieu of srand() or 
// srandom(), since random() is used to generate pure random numbers.
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_DISTRIBUTION_H
#define PIP_DISTRIBUTION_H

/*************  DISTRIBUTION SPECIFICATION  ***************/
typedef void   (pip_dist_init)(pip_var *var, HeapTupleHeader params);
typedef float8 (pip_dist_gen) (pip_var *var, int64 seed);
typedef float8 (pip_dist_pdf) (pip_var *var, float8 point);
typedef float8 (pip_dist_cdf) (pip_var *var, float8 point);
typedef float8 (pip_dist_icdf)(pip_var *var, float8 point);
typedef int    (pip_dist_in)  (pip_var *var, char *str);
typedef int    (pip_dist_out) (pip_var *var, int len, char *str);

typedef struct pip_functable_entry {
  char          *name;
  int            size;
  pip_dist_init *init;
  pip_dist_gen  *gen;
  pip_dist_pdf  *pdf;
  pip_dist_cdf  *cdf;
  pip_dist_icdf *icdf;
  pip_dist_in   *in;
  pip_dist_out  *out;
  bool           joint;
} pip_functable_entry;

#define DECLARE_PIP_DISTRIBUTION(dist_name) pip_functable_entry dist_name##_functable
#define PVAR_SIZE(group_id) (sizeof(pip_var) + pip_distributions[group_id]->size)

//note that the following macros take a pvar_id, and not a pvar *.  Thus, most invocations
//will be of the form PVAR_HAS_*(var->vid)
#define PVAR_HAS_PDF(pvar_id) \
  ((pvar_id.group < pip_distribution_count) ? \
    (pip_distributions[pvar_id.group]->pdf != NULL) : false)
#define PVAR_HAS_CDF(pvar_id) \
  ((pvar_id.group < pip_distribution_count) ? \
    (pip_distributions[pvar_id.group]->cdf != NULL) : false)
#define PVAR_HAS_ICDF(pvar_id) \
  ((pvar_id.group < pip_distribution_count) ? \
    (pip_distributions[pvar_id.group]->icdf != NULL) : false)
#define PVAR_HAS_2WAY_CDF(pvar_id) \
  ((PVAR_HAS_CDF(pvar_id)) && (PVAR_HAS_ICDF(pvar_id)))
#define PVAR_IS_JOINT(pvar_id) \
  (pip_distributions[pvar_id.group]->joint)

/*************  GLOBALS  ***************/
extern pip_functable_entry *pip_distributions[];
extern int                 pip_distribution_count;

/*************  UTILITY FUNCTIONS  ***************/

#define RANDOM_MAX (((unsigned int)(1<<31))-1)
#define float_random() ((1.0/(float8)RANDOM_MAX) * (float8)random())

/** Parameter Functions (pip.c) **/
float8 dist_param_float8(HeapTupleHeader params, int paramnum, float8 default_value);

/** Random Numbers (pip.c) **/
int64  pip_prng_step    (int64 seed);
int64  pip_prng_int     (int64 *seed);
float8 pip_prng_float   (int64 *seed);
void   pip_box_muller   (float8 *X, float8 *Y, int64 *seed); //dist/normal.c

/** Group Management **/
int    pip_group_lookup (char *name); //pvar.c

#endif
