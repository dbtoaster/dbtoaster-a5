/*
 * PIP general functions
 *
 *
 */

#ifndef PIP_H
#define PIP_H

#include "c.h"
#include "access/htup.h"
#include "nodes/nodes.h"

#include "list_utils.h"
#include "loglevels.h"
#include "types.h"
#include "dist.h"

#include "pvar.h"
#include "eqn.h"
#include "atom.h"
#include "sample_set.h"
#include "conf_tally.h"
#include "value_bundle.h"

/** Hooking operations (pip.c) **/
void _PG_init(void);

/** Integration operations (sample/integration.c) **/
float8 pip_compute_independent_probability(int clause_cnt, pip_atom **clause, int samples);
float8 pip_compute_expectation(pip_eqn *eqn, int clause_cnt, pip_atom **clause, int64 samples);
float8 pip_compute_expectation_conditionless(pip_eqn *eqn, int64 samples);

/** Constrained Sampling operations (sample/csampling.c) **/
pip_sample_set *pip_sample_by_clause   (int clause_cnt, pip_atom **clause, int sample_cnt, float8 *probability);
bool pip_sample_test_clause(pip_sample_set *samples, int i, int clause_cnt, pip_atom **clause);

/** Equation Solver (sample/solver.c) **/
//solve bound may return NAN if it is not possible to extract a simple bound from the clause.
float8 pip_solve_bound(pip_atom *clause, pip_var *var, bool *isupper);
//if tight is false, the returned bounds are not comprehensive, but may be better than [0,1]
bool pip_solve_cdf_bounds(pip_atom **atoms, int num_atoms, pip_var *var, float8 bounds[2], bool tight);

/** Debugging tools **/
#define DPING() elog(NOTICE, "%s(%s:%d)", __FUNCTION__, __FILE__, __LINE__)

#endif /* PIP_H */