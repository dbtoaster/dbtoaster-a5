/*
drop index LByOKPKSK;
drop index OByOKCK;
drop index OByOD;
drop index PByPK;
drop index CByCKNK;
drop index SBySKNK;
drop index NByNK;

/
*/

drop trigger refresh_lineitem;
drop trigger refresh_orders;
drop trigger refresh_customer;
drop trigger refresh_supplier;
drop trigger refresh_part;
drop trigger refresh_nation;

/

drop directory ssb4log;
exit;