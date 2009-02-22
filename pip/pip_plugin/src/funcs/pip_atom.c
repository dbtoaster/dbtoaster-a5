#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "eqn.h"


Datum   pip_atom_in   (PG_FUNCTION_ARGS)
{
  char      *str = PG_GETARG_CSTRING(0);
  pip_atom  *atom = NULL;
  
  pip_atom_parse(str, &atom, NULL);
  
  PG_RETURN_POINTER(atom);
}

Datum   pip_atom_out   (PG_FUNCTION_ARGS)
{
  pip_atom  *atom = (pip_atom *) PG_GETARG_BYTEA_P(0);
  char      *str;
  
  str = (char *) palloc(1000);

  pip_atom_sprint(str, 1000, atom);
  
  PG_RETURN_CSTRING(str);
}

Datum   pip_atom_create_gt_ee   (PG_FUNCTION_ARGS)
{
  pip_atom  *atom;
  pip_eqn   *left = (pip_eqn *)PG_GETARG_BYTEA_P(0),
            *right = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  atom = pip_atom_compose(left, right);
  
  PG_RETURN_POINTER(atom);
}
Datum   pip_atom_create_lt_ee   (PG_FUNCTION_ARGS)
{
  pip_atom  *atom;
  pip_eqn   *left = (pip_eqn *)PG_GETARG_BYTEA_P(0),
            *right = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  atom = pip_atom_compose(right, left);
  
  PG_RETURN_POINTER(atom);
}
Datum   pip_atom_create_gt_ef   (PG_FUNCTION_ARGS)
{
  pip_atom  *atom;
  pip_eqn   *left = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  float8     right = PG_GETARG_FLOAT8(1);
  
  atom = pip_atom_compose_gtf(left, right);
  
  PG_RETURN_POINTER(atom);
}
Datum   pip_atom_create_gt_fe   (PG_FUNCTION_ARGS)
{
  pip_atom  *atom;
  float8     left = PG_GETARG_FLOAT8(0);
  pip_eqn   *right = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  atom = pip_atom_compose_ltf(left, right);
  
  PG_RETURN_POINTER(atom);
}
Datum   pip_atom_create_lt_ef   (PG_FUNCTION_ARGS)
{
  pip_atom  *atom;
  pip_eqn   *left = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  float8     right = PG_GETARG_FLOAT8(1);

  atom = pip_atom_compose_ltf(right, left);
  
  PG_RETURN_POINTER(atom);
}
Datum   pip_atom_create_lt_fe   (PG_FUNCTION_ARGS)
{
  pip_atom  *atom;
  float8     left = PG_GETARG_FLOAT8(0);
  pip_eqn   *right = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  atom = pip_atom_compose_gtf(right, left);
  
  PG_RETURN_POINTER(atom);
}
