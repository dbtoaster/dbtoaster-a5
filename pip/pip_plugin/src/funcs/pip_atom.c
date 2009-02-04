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
  pip_atom  *atom;
  int        count, size, left_size = 0, right_size = 0;
  
  if((str[0] != '>') && (str[0] != '<')){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid atom type (must be < or >): '%c'", str[0])));
  }
  
  if((count = pip_eqn_cmpnt_parse(str+1, NULL, 0, &left_size)) < 0){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid atom string; left side unparsable ('%s';%d;%d)", str+1, count, left_size)));
  }
  if(pip_eqn_cmpnt_parse(str+1+count, NULL, 0, &right_size) < 0){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid atom string; right side unparsable ('%s';%d)", str+count+1, right_size)));
  }
  
  size = left_size + right_size + sizeof(pip_atom);
  
  atom = SPI_palloc(size);
  bzero(atom, size);
  SET_VARSIZE(atom, size);
  
  pip_eqn_cmpnt_parse(str+1,       atom->data, 0,         NULL);
  pip_eqn_cmpnt_parse(str+1+count, atom->data, left_size, NULL);
  
  atom->ptr_left  = (str[0] == '>') ? 0 : left_size;
  atom->ptr_right = (str[0] == '>') ? left_size : 0;
  
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
