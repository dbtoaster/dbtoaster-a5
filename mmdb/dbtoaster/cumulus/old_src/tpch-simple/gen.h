
#ifndef GEN_H_SHIELD
#define GEN_H_SHIELD

#ifdef NEED_TDEFS
/*
* flat file print functions; used with -F(lat) option
*/
int pr_cust (customer_t * c, int mode);
int pr_line (order_t * o, int mode);
int pr_order (order_t * o, int mode);
int pr_part (part_t * p, int mode);
int pr_psupp (part_t * p, int mode);
int pr_supp (supplier_t * s, int mode);
int pr_order_line (order_t * o, int mode);
int pr_part_psupp (part_t * p, int mode);
int pr_nation (code_t * c, int mode);
int pr_region (code_t * c, int mode);

/*
* inline load functions; used with -D(irect) option
*/
int ld_cust (customer_t * c, int mode);
int ld_line (order_t * o, int mode);
int ld_order (order_t * o, int mode);
int ld_part (part_t * p, int mode);
int ld_psupp (part_t * p, int mode);
int ld_supp (supplier_t * s, int mode);
int ld_order_line (order_t * o, int mode);
int ld_part_psupp (part_t * p, int mode);
int ld_nation (code_t * c, int mode);
int ld_region (code_t * c, int mode);

/*
* seed generation functions; used with '-O s' option
*/
long sd_cust (int child, DSS_HUGE skip_count);
long sd_line (int child, DSS_HUGE skip_count);
long sd_order (int child, DSS_HUGE skip_count);
long sd_part (int child, DSS_HUGE skip_count);
long sd_psupp (int child, DSS_HUGE skip_count);
long sd_supp (int child, DSS_HUGE skip_count);
long sd_order_line (int child, DSS_HUGE skip_count);
long sd_part_psupp (int child, DSS_HUGE skip_count);

/*
* header output functions); used with -h(eader) option
*/
int hd_cust (FILE * f);
int hd_line (FILE * f);
int hd_order (FILE * f);
int hd_part (FILE * f);
int hd_psupp (FILE * f);
int hd_supp (FILE * f);
int hd_order_line (FILE * f);
int hd_part_psupp (FILE * f);
int hd_nation (FILE * f);
int hd_region (FILE * f);

/*
* data verfication functions; used with -O v option
*/
int vrf_cust (customer_t * c, int mode);
int vrf_line (order_t * o, int mode);
int vrf_order (order_t * o, int mode);
int vrf_part (part_t * p, int mode);
int vrf_psupp (part_t * p, int mode);
int vrf_supp (supplier_t * s, int mode);
int vrf_order_line (order_t * o, int mode);
int vrf_part_psupp (part_t * p, int mode);
int vrf_nation (code_t * c, int mode);
int vrf_region (code_t * c, int mode);
#endif //NEED_TDEFS

// driver.c
void load_dists (void);
void gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num);

#ifdef NEED_LIFENOISE
static char lnoise[4] = {'|', '/', '-', '\\' };
#define LIFENOISE(n, var)	\
	if (verbose > 0) fprintf(stderr, "%c\b", lnoise[(var%LN_CNT)])
#endif

#ifndef DISABLE_PINGS
#define PING() printf("%s:%d %s()\n", __FILE__, __LINE__, __FUNCTION__)
#else
#define PING()
#endif

#endif //GEN_H_SHIELD
