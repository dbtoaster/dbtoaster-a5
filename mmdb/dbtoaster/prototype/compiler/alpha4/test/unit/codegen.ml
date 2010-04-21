
open Util;;
open M3;;

(****************************************************************************)
(* Query Definitions *)

(* VWAP *)
let vwap_1 = 
  ( ("QUERY_1_1", [], [], (mk_c 0., ())),
    (mk_prod
      (mk_if
        (mk_lt
          (mk_sum
            (mk_ma ("QUERY_1_1__3",["B1__PRICE"],[],(
              (mk_if
                (mk_lt (mk_v "B1__PRICE") (mk_v "B2__PRICE"))
                (mk_prod 
                  (mk_ma ("INPUT_MAP_BIDS", [], ["B2__PRICE";"B2__VOLUME"],(
                    mk_c 0.,()
                  )))
                  (mk_v "B2__VOLUME")
                )
              ),()
            )))
            (mk_prod
              (mk_v "QUERY_1_1BIDS_B1__VOLUME")
              (mk_if 
                (mk_lt (mk_v "B1__PRICE") (mk_v "QUERY_1_1BIDS_B1__PRICE" ))
                (mk_c 1.)
              ) 
            )
          )
          (mk_sum 
            (mk_prod 
              (mk_c 0.25) 
              (mk_ma ("QUERY_1_1__2", [], [], (mk_c 0.,())))
            )
            (mk_prod (mk_c 0.25) (mk_v "QUERY_1_1BIDS_B1__VOLUME"))
          )
        )
        (mk_prod
          (mk_if
            (mk_eq (mk_v "B1__PRICE") (mk_v "QUERY_1_1BIDS_B1__PRICE"))
            (mk_v "B1__PRICE")
          )
          (mk_v "QUERY_1_1BIDS_B1__VOLUME")
        )
      )
      (mk_prod
        (mk_c (-1.0))
        (mk_if
          (mk_leq 
            (mk_prod
              (mk_c 0.25)
              (mk_ma ("QUERY_1_1__2", [],[], (mk_c 0.,())))
            )
            (mk_ma ("QUERY_1_1__3",["B1__PRICE"],[],(
              (mk_if
                (mk_lt (mk_v "B1__PRICE") (mk_v "B2__PRICE"))
                (mk_prod
                  (mk_ma ("INPUT_MAP_BIDS",[],["B2__PRICE"; "B2__VOLUME"],(
                    mk_c 0.,())))
                  (mk_v "B2__VOLUME")
                )
              ),()))
            )
             
          ) 
          ( mk_c 1. )
        )(**)
      )
    ,()),());;

(****************************************************************************)
Debug.log_unit_test "M3 Printing" (fun x->x)
  (M3Common.pretty_print_stmt vwap_1)
"( QUERY_1_1[  ][  ] := 0. ) +=
 ( ( IF ( ( ( ( QUERY_1_1__3[ B1__PRICE ][  ] := 
                ( IF ( ( B1__PRICE < B2__PRICE ) ) 
                  THEN ( ( ( INPUT_MAP_BIDS[  ][ B2__PRICE; B2__VOLUME ] := 0. ) * B2__VOLUME ) ))
               )
               + 
              ( QUERY_1_1BIDS_B1__VOLUME * ( IF ( ( B1__PRICE < QUERY_1_1BIDS_B1__PRICE ) ) THEN ( 1. )) ) )
             < 
            ( ( 0.25 * ( QUERY_1_1__2[  ][  ] := 0. ) ) + ( 0.25 * QUERY_1_1BIDS_B1__VOLUME ) ) )
          
      ) 
     THEN ( ( ( IF ( ( B1__PRICE = QUERY_1_1BIDS_B1__PRICE ) ) THEN ( B1__PRICE )) * QUERY_1_1BIDS_B1__VOLUME ) ))
    * 
   ( -1. * 
     ( IF ( ( ( 0.25 * ( QUERY_1_1__2[  ][  ] := 0. ) ) <= 
              ( QUERY_1_1__3[ B1__PRICE ][  ] := 
                ( IF ( ( B1__PRICE < B2__PRICE ) ) 
                  THEN ( ( ( INPUT_MAP_BIDS[  ][ B2__PRICE; B2__VOLUME ] := 0. ) * B2__VOLUME ) ))
               )
             )
            
        ) 
       THEN ( 1. ))
    )
  )

";;