#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/typcache.h"
#include "utils/memutils.h"
#include "access/heapam.h"

#include "pip.h"
#include "eqn.h"
#include "atom.h"

static Oid pip_atom_oid();

static Oid pip_atom_oid()
{
  static Oid PIP_ATOM_OID = (Oid)50500;
  
  //a little bit of a hack, but it gets the job done.  This uses SPI to issue a query that
  //generates a dummy pip_atom and extracts an OID based on the response.  It's ugly, and 
  //it's probably not that fast... but it only needs to happen once per reset.
  if(PIP_ATOM_OID == 0){
    int connect_status;
    elog(NOTICE, "Need to look up PIP_ATOM_OID");
    connect_status = SPI_connect();
    if(connect_status == SPI_ERROR_CONNECT){ 
      //oops, we're already connected.  This is not how push/pop are supposed to be used
      //buuuut... forcing people to put a push/pop around calls to extract_atoms is uglier
      SPI_push();
      SPI_connect();
    }
    if(SPI_execute("SELECT cast('>(25.0)(25.0)' AS pip_atom)", true, 1) == SPI_OK_SELECT){
      PIP_ATOM_OID = SPI_gettypeid(SPI_tuptable->tupdesc, 1);
    }
    elog(NOTICE, "  PIP_ATOM_OID = %d", (int)PIP_ATOM_OID);
    SPI_finish();
    if(connect_status == SPI_ERROR_CONNECT){
      SPI_pop();
    }
  }
  return PIP_ATOM_OID;
}

int pip_atom_sprint(char *str, int len, pip_atom *atom)
{
  int c = 1;
  str[0] = '>';
  c += pip_eqn_cmpnt_sprint(str+c, len-c, atom->data, atom->ptr_left);
  c += pip_eqn_cmpnt_sprint(str+c, len-c, atom->data, atom->ptr_right);
  return c;
}
void pip_atom_log(pip_atom *atom)
{
  char str[400];
  pip_atom_sprint(str, 400, atom);
  elog(NOTICE, "%s", str);
}

//The code for this function is based off of executor/execQual.c: GetAttributeByName()
int pip_extract_clause(HeapTupleHeader row, pip_atom ***out)
{
  int               i, count = 0;
	Oid			          tupType;
	int32		          tupTypmod;
	TupleDesc	        tupDesc;
	HeapTupleData     tmptup;
	Datum             atom_datum;
  bool              isnull;
	
	tupType = HeapTupleHeaderGetTypeId(row);
	tupTypmod = HeapTupleHeaderGetTypMod(row);
	tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
  
	tmptup.t_len = HeapTupleHeaderGetDatumLength(row);
	tmptup.t_tableOid = InvalidOid;
	tmptup.t_data = row;
  
  if(*out == NULL){
    for(i = 0; i < tupDesc->natts; i++){
      if(tupDesc->attrs[i]->atttypid == pip_atom_oid()){ 
        count++;
      }
    }
    *out = palloc0(count * sizeof(pip_atom *));
    count = 0;
  }
  
  for(i = 0; i < tupDesc->natts; i++){
    if(tupDesc->attrs[i]->atttypid == pip_atom_oid()){
      atom_datum = heap_getattr(&tmptup,
                      tupDesc->attrs[i]->attnum,
                      tupDesc,
                      &isnull);
      if(!isnull){
        pip_atom *atom = (pip_atom *)PG_DETOAST_DATUM(atom_datum);
        (*out)[count] = palloc0(VARSIZE(atom));
        memcpy((*out)[count], atom, VARSIZE(atom));
        count++;
      }
    }
  }

	ReleaseTupleDesc(tupDesc);
  
  return count;
}

void pip_clause_to_cset(int clause_cnt, pip_atom **clause, pip_cset *set)
{
  int i;
  pip_var *left, *right;
  
  pip_cset_init(set, (pip_sort_comparator *)&pip_variable_sort);
  
  for(i = 0; i < clause_cnt; i++){
    left  = pip_eqn_cmpnt_to_cset(clause[i]->data, clause[i]->ptr_left , set);
    right = pip_eqn_cmpnt_to_cset(clause[i]->data, clause[i]->ptr_right, set);
    if(left && right){
      pip_cset_link(set, left, right);
    }
  }
}

bool pip_atom_evaluate_seed(pip_atom *atom, int64 seed)
{
  float8 left, right;
  left =  pip_eqn_cmpnt_evaluate_seed(atom->data, atom->ptr_left , seed);
  right = pip_eqn_cmpnt_evaluate_seed(atom->data, atom->ptr_right, seed);
  return left > right;  
}
bool pip_atom_evaluate_sample(pip_atom *atom, pip_sample_set *set, int sample)
{
  float8 left, right;
  left =  pip_eqn_cmpnt_evaluate_sample(atom->data, atom->ptr_left , set, sample);
  right = pip_eqn_cmpnt_evaluate_sample(atom->data, atom->ptr_right, set, sample);
  return left > right;  
}

int pip_atom_has_var(pip_atom *atom, pip_var *var)
{
  if(pip_eqn_cmpnt_has_var(atom->data, atom->ptr_left , var)) return  1;
    else
  if(pip_eqn_cmpnt_has_var(atom->data, atom->ptr_right, var)) return -1;
    else
  return 0;
}

typedef struct pip_lineage_gathering_state {
  int        atoms_complete;
  int        clause_cnt;
  pip_atom **clause;
} pip_lineage_gathering_state;
static int pip_gather_atoms(pip_cset *variables, pip_var *var, pip_lineage_gathering_state *info);
static int pip_gather_atoms(pip_cset *variables, pip_var *var, pip_lineage_gathering_state *info)
{
  int i;
  pip_atom *tmp_atom;
  for(i = info->atoms_complete; i < info->clause_cnt; i++){
    if(pip_atom_has_var(info->clause[i], var)){
      if(i != info->atoms_complete){
        tmp_atom = info->clause[i];
        info->clause[i] = info->clause[info->atoms_complete];
        info->clause[info->atoms_complete] = tmp_atom;
      }
      info->atoms_complete++;
    }
  }

  return 0;
}

int pip_group_atoms(pip_cset *variables, pip_cset_element *group, int clause_cnt, pip_atom **clause, int atoms_complete)
{
  pip_lineage_gathering_state info;
  info.atoms_complete = atoms_complete;
  info.clause_cnt     = clause_cnt;
  info.clause         = clause;
  pip_cset_iterate_group(variables, group, (pip_cset_iterator *)&pip_gather_atoms, &info);
  return info.atoms_complete;
}

pip_atom *pip_atom_compose_cmpnt(pip_eqn_component *left, int left_size, pip_eqn_component *right, int right_size)
{
  pip_atom  *atom;
  int        size = left_size + right_size + sizeof(pip_atom);
  
  atom = SPI_palloc(size);
  bzero(atom, size);
  SET_VARSIZE(atom, size);
  
  memcpy(atom->data, left, left_size);
  memcpy(atom->data+left_size, right, right_size);
  pip_eqn_cmpnt_update_pointers(atom->data+left_size, 0, left_size);
  
  atom->ptr_left = 0;
  atom->ptr_right = left_size;
  return atom;
}

pip_atom *pip_atom_compose(pip_eqn *left, pip_eqn *right)
{
  return pip_atom_compose_cmpnt((pip_eqn_component *)left->data, VARSIZE(left)-sizeof(pip_eqn), (pip_eqn_component *)right->data, VARSIZE(right)-sizeof(pip_eqn));
}
pip_atom *pip_atom_compose_gtf(pip_eqn *left, float8 right)
{
  pip_eqn_component cmpnt;
  
  bzero(&cmpnt, sizeof(pip_eqn_component));
  cmpnt.type = PIP_EQN_CONST;
  cmpnt.val.c = right;
  
  return pip_atom_compose_cmpnt((pip_eqn_component *)left->data, VARSIZE(left)-sizeof(pip_eqn), &cmpnt, sizeof(pip_eqn_component));
}
pip_atom *pip_atom_compose_ltf(float8 left, pip_eqn *right)
{
  pip_eqn_component cmpnt;
  
  bzero(&cmpnt, sizeof(pip_eqn_component));
  cmpnt.type = PIP_EQN_CONST;
  cmpnt.val.c = left;
  
  return pip_atom_compose_cmpnt(&cmpnt, sizeof(pip_eqn_component), (pip_eqn_component *)right->data, VARSIZE(right)-sizeof(pip_eqn));
}

