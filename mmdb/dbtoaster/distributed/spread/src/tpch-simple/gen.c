#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <getopt.h>

#define DECLARER				/* EXTERN references get defined here */
#include "shared.h"
#include "dss.h"
#include "dsstypes.h"
#include "config.h"
#define NEED_TDEFS
#include "gen.h"

DSS_HUGE rowcnt = 0, minrow = 0;
long upd_num = 0;
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */


tdef tdefs[] =
{
	{"part.tbl", "part table", 200000, hd_part,
		{pr_part, ld_part}, sd_part, vrf_part, PSUPP, 0},
	{"partsupp.tbl", "partsupplier table", 200000, hd_psupp,
		{pr_psupp, ld_psupp}, sd_psupp, vrf_psupp, NONE, 0},
	{"supplier.tbl", "suppliers table", 10000, hd_supp,
		{pr_supp, ld_supp}, sd_supp, vrf_supp, NONE, 0},
	{"customer.tbl", "customers table", 150000, hd_cust,
		{pr_cust, ld_cust}, sd_cust, vrf_cust, NONE, 0},
	{"orders.tbl", "order table", 150000, hd_order,
		{pr_order, ld_order}, sd_order, vrf_order, LINE, 0},
	{"lineitem.tbl", "lineitem table", 150000, hd_line,
		{pr_line, ld_line}, sd_line, vrf_line, NONE, 0},
	{"orders.tbl", "orders/lineitem tables", 150000, hd_order_line,
		{pr_order_line, ld_order_line}, sd_order, vrf_order_line, LINE, 0},
	{"part.tbl", "part/partsupplier tables", 200000, hd_part_psupp,
		{pr_part_psupp, ld_part_psupp}, sd_part, vrf_part_psupp, PSUPP, 0},
	{"nation.tbl", "nation table", NATIONS_MAX, hd_nation,
		{pr_nation, ld_nation}, NO_LFUNC, vrf_nation, NONE, 0},
	{"region.tbl", "region table", NATIONS_MAX, hd_region,
		{pr_region, ld_region}, NO_LFUNC, vrf_region, NONE, 0},
};

///////////// Customer presence trie
typedef struct cust_trie {
  struct cust_trie *next[2];
} cust_trie;

static cust_trie cust_trie_root;
static cust_trie *batch_trie_alloc;
static int batch_trie_alloc_remaining = 0;

int test_and_set_customer(DSS_HUGE cid){
  cust_trie *curr = &cust_trie_root;
  int depth = 0; 
  int ret = 0;
  int bit;
  for(depth = sizeof(DSS_HUGE)-1; depth >= 0; depth--){
    bit = (cid >> depth) & 0x1;
    if(!curr->next[bit]){
      if(batch_trie_alloc_remaining <= 0){
        batch_trie_alloc_remaining = 1000;
        batch_trie_alloc = malloc(1000 * sizeof(cust_trie));
      }
      curr->next[bit] = batch_trie_alloc;
      batch_trie_alloc++;
      batch_trie_alloc_remaining --;
      ret = 0;
    } else {
      ret = 1;
    }
    curr = curr->next[bit];
  }
  return ret;
}

///////////// Output code

void clean_string(char *str, int lim){
  int i;
  for(i = 0; (str[i] != '\0') && (i < lim); i++){
    if(str[i] == ','){ str[i] = '.'; }
  }
}

void print_lineitem(line_t *l){
  clean_string(l->cdate       ,DATE_LEN);
  clean_string(l->sdate       ,DATE_LEN);
  clean_string(l->rdate       ,DATE_LEN);
  clean_string(l->shipinstruct,MAXAGG_LEN + 1);
  clean_string(l->shipmode    ,MAXAGG_LEN + 1);
  clean_string(l->comment     ,L_CMNT_MAX + 1);
  printf("LINEITEMS(%llu, %llu, %llu, %llu, %llu, %llu, %llu, %llu, %c, %c, %s, %s, %s, %s, %s, %s)\n",
    l->okey,
    l->partkey,
    l->suppkey,
    l->lcnt,
    l->quantity,
    l->eprice,
    l->discount,
    l->tax,
    l->rflag[0],
    l->lstatus[0],
    l->cdate,
    l->sdate,
    l->rdate,
    l->shipinstruct,
    l->shipmode,
    l->comment
  );
}

void print_customer(customer_t *c){
  clean_string(c->name      , C_NAME_LEN + 3);
  clean_string(c->address   , C_ADDR_MAX + 1);
  clean_string(c->phone     , PHONE_LEN + 1);
  clean_string(c->mktsegment, MAXAGG_LEN + 1);
  clean_string(c->comment   , C_CMNT_MAX + 1);
  printf("CUSTOMERS(%llu, %s, %s, %llu, %s, %llu, %s, %s)\n",
    c->custkey,
    c->name,
    c->address,
    c->nation_code,
    c->phone,
    c->acctbal,
    c->mktsegment,
    c->comment
    );
}

void print_order(order_t *o){
  int l;
  
  clean_string(o->odate, DATE_LEN);
  clean_string(o->opriority, MAXAGG_LEN + 1);
  clean_string(o->clerk, O_CLRK_LEN + 1);
  clean_string(o->comment, O_CMNT_MAX + 1);
  printf("ORDERS(%llu, %llu, %c, %llu, %s, %s, %s, %lu, %s)\n", 
        o->okey, 
        o->custkey, 
        o->orderstatus,
        o->totalprice,
        o->odate,
        o->opriority,
        o->clerk,
        o->spriority,
        o->comment);
  for(l = 0; l < o->lines; l++){
    print_lineitem(&(o->l[l]));
  }
}

///////////// OPTIONS
static int number_of_iterations = 1000;

void parse_args(int argc, char** argv){
  int opt;
  while((opt = getopt(argc, argv, "n:")) >= 0){
    switch(opt){
      case 'n':
        if(optarg[0] == 'u') { number_of_iterations = -1; }
        else { number_of_iterations = atoi(optarg); }
        break;
    }
  }
}

int main(int argc, char** argv) {
	DSS_HUGE i;
	
	if(chdir("data")){
    perror("Unable to open data directory");
    exit(-1);
	}
	
  table = (1 << CUST) |
		(1 << SUPP) |
		(1 << NATION) |
		(1 << REGION) |
		(1 << PART_PSUPP) |
		(1 << ORDER_LINE);
	force = 0;
    insert_segments=0;
    delete_segments=0;
    insert_orders_segment=0;
    insert_lineitem_segment=0;
    delete_segment=0;
	verbose = 0;
	columnar = 0;
	set_seeds = 0;
	header = 0;
	direct = 0;
	scale = 1;
	//flt_scale = 1.0;
	updates = 0;
	refresh = UPD_PCT;
	step = 0;
	tdefs[ORDER].base      *= ORDERS_PER_CUST;
	tdefs[LINE].base       *= ORDERS_PER_CUST;
	tdefs[ORDER_LINE].base *= ORDERS_PER_CUST;
	fnames = 0;
	db_name = NULL;
	gen_sql = 0;
	gen_rng = 0;
	children = 1;
	d_path = NULL;
	
	parse_args(argc, argv);
	
	load_dists ();
	
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;
  
  set_state (ORDER, scale, children, children + 1, &i); 
  rowcnt = (int)(tdefs[ORDER_LINE].base / 10000 * UPD_PCT);
  
  
  for (i=1; i < step; i++)
  {
    sd_order(0, rowcnt);
    sd_line(0, rowcnt);
  }
  for(i = 0; (i < number_of_iterations) || (number_of_iterations < 0); i++)
  {
    order_t order;
    customer_t cust;
    
    insert_orders_segment=0;
    insert_lineitem_segment=0;
    delete_segment=0;
    
    mk_order(i, &order, 1);
    if(!test_and_set_customer(order.custkey)){
      mk_cust(order.custkey, &cust);
      print_customer(&cust);
    }
    print_order(&order);
  }
  
  return 0;
}
