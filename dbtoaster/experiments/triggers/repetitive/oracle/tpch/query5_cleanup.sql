/*
drop index LByOKSK;
drop index OByOKCK;
drop index CByCKNK;
drop index SBySKNK;
drop index NByNKRK;
drop index RByRK;

/
*/

drop trigger refresh_lineitem;
drop trigger refresh_orders;
drop trigger refresh_customer;
drop trigger refresh_supplier;
drop trigger refresh_nation;
drop trigger refresh_region;

/

drop directory q5log;
exit;