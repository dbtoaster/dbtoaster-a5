<?php
  $simple = array("inequality_selfjoin","invalid_schema_fn","m3k3unable2escalate","miraculous_minus","miraculous_minus2","r_aggcomparison","r_aggofnested","r_aggofnestedagg","r_agtb","r_agtbexists","r_avg","r_bigsumstar","r_btimesa","r_btimesacorrelated","r_count","r_count_of_one","r_count_of_one_prime","r_deepscoping","r_divb","r_existsnestedagg","r_gbasumb","r_gtealldynamic","r_gtesomedynamic","r_gtsomedynamic","r_impossibleineq","r_indynamic","r_ineqandeq","r_instatic","r_lift_of_count","r_ltallagg","r_ltallavg","r_ltallcorravg","r_ltalldynamic","r_multinest","r_multiors","r_natselfjoin","r_nogroupby","r_nonjoineq","r_possibleineq","r_possibleineqwitheq","r_selectstar","r_simplenest","r_smallstar","r_starofnested","r_starofnestedagg","r_sum_gb_all_out_of_aggregate","r_sum_gb_out_of_aggregate","r_sum_out_of_aggregate","r_sumadivsumb","r_sumdivgrp","r_sumnestedintarget","r_sumnestedintargetwitheq","r_sumoutsideofagg","r_sumstar","r_union","r_unique_counts_by_a","rr_ormyself","rs","rs_cmpnest","rs_column_mapping_1","rs_column_mapping_2","rs_column_mapping_3","rs_eqineq","rs_ineqonnestedagg","rs_inequality","rs_ineqwithnestedagg","rs_joinon","rs_joinwithnestedagg","rs_natjoin","rs_natjoinineq","rs_natjoinpartstar","rs_selectconstcmp","rs_selectpartstar","rs_selectstar","rs_simple","rs_stringjoin","rst","rstar","rtt_or_with_stars","singleton_renaming_conflict","ss_math","t_lifttype");
  $employee = array("query01","query01a","query02","query02a","query03","query03a","query04","query04a","query05","query06","query07","query08","query08a","query09","query09a","query10","query10a",
          "query11b",#"query11","query11a","query12","query15","query35b","query36b",
          "query12a","query13","query14","query16","query16a","query17a","query22","query23a","query24a","query35c","query36c","query37","query38a","query39","query40","query45","query46","query47","query47a","query48","query49","query50","query51",
          #"query52","query53","query56","query57","query58","query62","query63","query64","query65","query66","query66a",
          "query52a","query53a","query54","query55","query56a","query57a","query58a","query59","query60","query61","query62a","query63a","query64a","query65a");
  $tpch = array("tpch1","tpch2","tpch3","tpch4","tpch5","tpch6","tpch7","tpch8",
          "tpch9","tpch10","tpch11","tpch11a","tpch11c","tpch12","tpch13","tpch14","tpch15",
          "tpch16","tpch17","tpch17a","tpch18","tpch18a","tpch19","tpch20","tpch21","tpch22","tpch22a"#,"ssb4"
          );
  $finance = array("axfinder","brokerspread","brokervariance","pricespread","vwap");
  $mddb = array("mddb1","mddb2");
  $zeus = array("11564068","12811747","37494577","39765730","48183500","52548748","59977251","75453299","94384934","95497049","96434723");

function identity($n)
{
    return $n;
}
function zeusConv($n)
{
    return "zeus".$n;
}

  $code = array(
          "simple" => array_combine($simple, $simple),
          "employee" =>
            array("query01" => "Employee01","query01a" => "Employee01a","query02" => "Employee02","query02a" => "Employee02a","query03" => "Employee03","query03a" => "Employee03a","query04" => "Employee04","query04a" => "Employee04a","query05" => "Employee05","query06" => "Employee06","query07" => "Employee07","query08" => "Employee08","query08a" => "Employee08a","query09" => "Employee09","query09a" => "Employee09a","query10" => "Employee10","query10a",
              "query11b",#"query11" => "Employee11","query11a" => "Employee11a","query12" => "Employee12","query15" => "Employee15","query35b" => "Employee35b","query36b",
              "query12a" => "Employee12a","query13" => "Employee13","query14" => "Employee14","query16" => "Employee16","query16a" => "Employee16a","query17a" => "Employee17a","query22" => "Employee22","query23a" => "Employee23a","query24a" => "Employee24a","query35c" => "Employee35c","query36c" => "Employee36c","query37" => "Employee37","query38a" => "Employee38a","query39" => "Employee39","query40" => "Employee40","query45" => "Employee45","query46" => "Employee46","query47" => "Employee47","query47a" => "Employee47a","query48" => "Employee48","query49" => "Employee49","query50" => "Employee50","query51",
              #"query52" => "Employee52","query53" => "Employee53","query56" => "Employee56","query57" => "Employee57","query58" => "Employee58","query62" => "Employee62","query63" => "Employee63","query64" => "Employee64","query65" => "Employee65","query66" => "Employee66","query66a",
              "query52a" => "Employee52a","query53a" => "Employee53a","query54" => "Employee54","query55" => "Employee55","query56a" => "Employee56a","query57a" => "Employee57a","query58a" => "Employee58a","query59" => "Employee59","query60" => "Employee60","query61" => "Employee61","query62a" => "Employee62a","query63a" => "Employee63a","query64a" => "Employee64a","query65a"),
          "tpch" => array_combine($tpch, $tpch),
          "finance" => array_combine($finance, $finance),
          "mddb" => array_combine($mddb, $mddb),
          "zeus" => array_combine($zeus, array_map("zeusConv", $zeus))
  );
  $query_group_color = array("simple" => "red",
          "employee" => "light",
          "tpch" => "dark",
          "finance" => "blue",
          "mddb" => "green",
          "zeus" => "red");

  $is_query_selected = False;
  if(isset($_GET['q'])) {
    $is_query_selected = True;
    $query = $_GET['q'];
    if(in_array($query,$simple)) {
      $query_group = "simple";
    } else if(in_array($query,$employee)) {
      $query_group = "employee";
    } else if(in_array($query,$tpch)) {
      $query_group = "tpch";
    } else if(in_array($query,$finance)) {
      $query_group = "finance";
    } else if(in_array($query,$mddb)) {
      $query_group = "mddb";
    } else if(in_array($query,$zeus)) {
      $query_group = "zeus";
    } else {
      $is_query_selected = False;
    }
    if($is_query_selected) {
      $query_code = ucfirst(str_replace("_","",$code[$query_group][$query]));
      if($query_group == "tpch") {
        $query = str_replace("tpch","query",$query);
      } else if($query_group == "mddb") {
        $query = str_replace("mddb","query",$query);
      }
      $query_color = $query_group_color[$query_group];
    }
  }

  if(!$is_query_selected) {
?>

<?php if(!isset($now_building_distro)) { ?>
<script type="text/javascript">
window.onload = function() {
  $( ".sbutton" ).on( "click", function() {
    location.href='index.php?page=samples&q='+$( this ).text();
  });
};
<?php } else { ?>
<script type="text/javascript">
window.onload = function() {
  $( ".sbutton" ).on( "click", function() {
    location.href='samples_'+ $( this ).attr("data-id") + '_' + $( this ).text() + '.html';
  });
};
<?php } ?>
</script>

<p>In this page, you will find a list of sample queries which we used DBToaster for generating IVM programs in both C++ and Scala. </p>

<p><i>Note:</i> The generated IVM programs are not meant to be human readable, and are presented for giving you an impression of the kind of IVM programs generated for each query.</p>

<?= chapter("TPC-H queries") ?>
<?php foreach ($tpch as $q) { ?>

<div class="sbutton dark center", data-id="tpch"><?=$q ?></div>
<?php } ?>

<?= chapter("Finance queries") ?>
<?php foreach ($finance as $q) { ?>
<div class="sbutton blue center", data-id="finance"><?=$q ?></div>
<?php } ?>

<?= chapter("Simple queries") ?>
<?php foreach ($simple as $q) { ?>
<div class="sbutton red center", data-id="simple"><?=$q ?></div>
<?php } ?>

<?= chapter("Employee queries") ?>
<?php foreach ($employee as $q) { ?>
<div class="sbutton light center", data-id="employee"><?=$q ?></div>
<?php } ?>

<?= chapter("MDDB queries") ?>
<?php foreach ($mddb as $q) { ?>
<div class="sbutton green center", data-id="mddb"><?=$q ?></div>
<?php } ?>

<?= chapter("Zeus queries") ?>
<?php foreach ($zeus as $q) { ?>
<div class="sbutton red center", data-id="zeus"><?=$q ?></div>
<?php } ?>

<?php } else { ?>

<table id="samplesTbl" style="border:0;width:100%;height:78%;margin-bottom:20px;">
  <tr style="height: 5%;">
<?php if($query_group == "tpch" || $query_group == "mddb") { ?>
    <td>
      <a href="htmlsql/<?=$query_group ?>/schemas.sql.html">SQL Schema:</a>
      <iframe src="htmlsql/<?=$query_group ?>/schemas.sql.html" style="border:0;width:100%;height:100%;"></iframe>
    </td>
    <td>
      <a href="htmlsql/<?=$query_group ?>/<?=$query ?>.sql.html">SQL Query:</a>
<?php } else { ?>
    <td colspan="2">
      <a href="htmlsql/<?=$query_group ?>/<?=$query ?>.sql.html">SQL Schema and Query:</a>
<?php } ?>
      <iframe src="htmlsql/<?=$query_group ?>/<?=$query ?>.sql.html" style="border:0;width:100%;height:100%;"></iframe>
    </td>
  </tr>
  <tr style="height: 100%;">
    <td style="padding-top:2%;">
      <a href="htmlcode/<?=$query_code ?>.hpp.html">C++ IVM Program:</a>
      <iframe src="htmlcode/<?=$query_code ?>.hpp.html" style="border:0;width:100%;height:100%;"></iframe>
    </td>
    <td style="padding-top:2%;">
      <a href="htmlcode/<?=$query_code ?>.scala.html">Scala IVM Program:</a>
      <iframe src="htmlcode/<?=$query_code ?>.scala.html" style="border:0;width:100%;height:100%;"></iframe>
    </td>
  </tr>
</table>

<?php if(!isset($now_building_distro)) { ?>
<script type="text/javascript">
window.onload = function() {
  $( ".titlebox" ).append(": <?=$query_code ?>").prepend("<div onclick=\"location.href='index.php?page=samples';\" style=\"margin-bottom:-40px;margin-top:0px;\" class=\"sbutton <?=$query_color ?> center\">Back</div> ")
  $( ".pagebody" ).css("margin-bottom","0")
  var footer = $( ".footer" ).detach();
  $( "#samplesTbl" ).detach().appendTo("#pageEndElem");
  footer.appendTo("#pageEndElem");
};
</script>
<?php } else { ?>
<script type="text/javascript">
window.onload = function() {
  $( ".titlebox" ).append(": <?=$query_code ?>").prepend("<div onclick=\"location.href='samples.html';\" style=\"margin-bottom:-40px;margin-top:0px;\" class=\"sbutton <?=$query_color ?> center\">Back</div> ")
  $( ".pagebody" ).css("margin-bottom","0")
  var footer = $( ".footer" ).detach();
  $( "#samplesTbl" ).detach().appendTo("#pageEndElem");
  footer.appendTo("#pageEndElem");
};
</script>
<?php } ?>

<?php } ?>