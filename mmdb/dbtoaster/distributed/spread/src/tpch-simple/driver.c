//
// Simplified TPC-H Generator for making stream Generator
//
// Liberally co... err... umm... derived from the official TPC-H Generator
//

#include <stdio.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "shared.h"
#include "dss.h"
#include "dsstypes.h"
#include "config.h"
#define NEED_LIFENOISE
#include "gen.h"


/*
* read the distributions needed in the benchamrk
*/
void
load_dists (void)
{
	read_dist (env_config (DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "colors", &colors);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "nations", &nations);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "regions", &regions);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "o_oprio",
		&o_priority_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "instruct",
		&l_instruct_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "category",
		&l_category_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);

	/* load the distributions that contain text generation */
	read_dist (env_config (DIST_TAG, DIST_DFLT), "nouns", &nouns);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "verbs", &verbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adjectives", &adjectives);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adverbs", &adverbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "auxillaries", &auxillaries);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "terminators", &terminators);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "articles", &articles);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "prepositions", &prepositions);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "grammar", &grammar);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "np", &np);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "vp", &vp);
	
}

/*
* generate a particular table
*/
void
gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num)
{
	static order_t o;
	supplier_t supp;
	customer_t cust;
	part_t part;
	code_t code;
	static int completed = 0;
	DSS_HUGE i;

	DSS_HUGE rows_per_segment=0;
	DSS_HUGE rows_this_segment=-1;
	DSS_HUGE residual_rows=0;

	if (insert_segments)
		{
		rows_per_segment = count / insert_segments;
		residual_rows = count - (rows_per_segment * insert_segments);
		}

	for (i = start; count; count--, i++)
	{
		LIFENOISE (1000, i);
		row_start(tnum);
      PING();

		switch (tnum)
		{
		case LINE:
		case ORDER:
  		case ORDER_LINE: 
      PING();
			mk_order (i, &o, upd_num % 10000);

      PING();
		  if (insert_segments  && (upd_num > 0)){
        PING();
        if((upd_num / 10000) < residual_rows)
          {
          PING();
          if((++rows_this_segment) > rows_per_segment) 
            {		
            PING();				
            rows_this_segment=0;
            upd_num += 10000;					
            }
          }
        else
          {
          PING();
          if((++rows_this_segment) >= rows_per_segment) 
            {
            PING();
            rows_this_segment=0;
            upd_num += 10000;
            }
          }
      }
      PING();
			if (set_seeds == 0){
        PING();
				if (validate){
          PING();
					tdefs[tnum].verify(&o, 0);
				}else{
          PING();
					tdefs[tnum].loader[direct] (&o, upd_num);
				}
      }
      PING();
			break;
		case SUPP:
			mk_supp (i, &supp);
			if (set_seeds == 0){
				if (validate)
					tdefs[tnum].verify(&supp, 0);
				else
					tdefs[tnum].loader[direct] (&supp, upd_num);
      }
			break;
		case CUST:
			mk_cust (i, &cust);
			if (set_seeds == 0){
				if (validate)
					tdefs[tnum].verify(&cust, 0);
				else
					tdefs[tnum].loader[direct] (&cust, upd_num);
      }
			break;
		case PSUPP:
		case PART:
  		case PART_PSUPP: 
			mk_part (i, &part);
			if (set_seeds == 0){
				if (validate)
					tdefs[tnum].verify(&part, 0);
				else
					tdefs[tnum].loader[direct] (&part, upd_num);
      }
			break;
		case NATION:
			mk_nation (i, &code);
			if (set_seeds == 0){
				if (validate)
					tdefs[tnum].verify(&code, 0);
				else
					tdefs[tnum].loader[direct] (&code, 0);
      }
			break;
		case REGION:
			mk_region (i, &code);
			if (set_seeds == 0){
				if (validate)
					tdefs[tnum].verify(&code, 0);
				else
					tdefs[tnum].loader[direct] (&code, 0);
		  }
			break;
		}
		row_stop(tnum);
		if (set_seeds && (i % tdefs[tnum].base) < 2)
		{
			printf("\nSeeds for %s at rowcount %ld\n", tdefs[tnum].comment, (long)i);
			dump_seeds(tnum);
		}
	}
	completed |= 1 << tnum;
}