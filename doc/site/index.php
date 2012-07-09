<?php
$pages = array(
  "home"             => "About", 
    "home_performance" => "Performance",
    "home_features"    => "Features",
    "home_research"    => "For Researchers",
    "home_contact"     => "Contact",
    "home_people"      => "The Team",
  "download"         => "Downloads",
  "docs"             => "Documentation",
    "docs_compiler"    => "Command-Line Reference",
    "docs_sql"         => "DBT-SQL Reference",
    "docs_stdlib"      => "DBT StdLib Reference",
    "docs_adaptors"    => "DBT Adaptors Reference",
    "docs_cpp"         => "C++ Code Generation",
    "docs_scala"       => "Scala Code Generation",
  "bugs"             => "Bug Reports",
);

if(isset($_GET["page"])){
  $page = $_GET["page"];
} else {
  $page = "home";
}
$subpage = $page;
if(isset($_GET["subpage"]) && ($_GET["subpage"] != "index")){
  $subpage .= "_".$_GET["subpage"];
}
$pagepath = "pages/$subpage.php";

$longtitle = "DBToaster";
$shorttitle = "DBToaster";
if(isset($pages[$page])){
  $shorttitle = $pages[$page];
  $longtitle .= ": $shorttitle";
}
if(isset($pages[$subpage])){
  $shorttitle = $pages[$subpage];
  $longtitle .= " - $shorttitle";
}

function add_anchor($text, $anchor){
  if($anchor == null){ 
    return $text; 
  } else { 
    return "<a name=\"$anchor\">$text</a>"; 
  }
}

$chapter = 0; $section = 0; $subsection = 0;
function chapter($title, $anchor = null){
  global $chapter,$section,$subsection;
  $chapter ++;
  $section = 0;
  $subsection = 0;
  return add_anchor("<h3>$chapter. $title</h3>", $anchor);
}
function section($title, $anchor = null){
  global $chapter,$section,$subsection;
  $section ++;
  $subsection = 0;
  return add_anchor("<h4>$chapter.$section. $title</h4>", $anchor);
}
function subsection($title, $anchor = null){
  global $chapter,$section,$subsection;
  $subsection ++;
  return add_anchor("<h5>$chapter.$section.$subsection. $title</h5>", $anchor);
}
function mk_link($text, $page, $subpage = null, $anchor = "", $atagextras = ""){
  global $pages, $now_building_distro;
  $fullpage = $page;
  if($subpage == null){
    $subpage = "";
  } else {
    $fullpage .= "_".$subpage;
    $subpage = "&subpage=$subpage";
  }
  if($text == null){
    $text = $pages[$fullpage];
  }
  if(isset($now_building_distro)){
    if($fullpage == "home") { $fullpage = "index"; }
    return "<a href=\"$fullpage.html$anchor\" $atagextras>$text</a>";
  } else {
    return "<a href=\"index.php?page=$page$subpage$anchor\" $atagextras>$text</a>";
  }
}
function chain_link_docs() {
  global $pages, $page, $subpage;
  
  if($page != "docs") { return ""; }

  $prev = "";
  $state = "prev";
  $ret = "";
  foreach($pages as $curr_stub => $curr_title) {
    if($subpage == $curr_stub){
      if($prev != ""){ $ret .= " &lt; ".$prev; }
      $state = "next";
    } else {
      $curr_page = explode("_", $curr_stub);
      $curr_subpage = implode("_", array_slice($curr_page, 1));
      $curr_page = $curr_page[0];
      
      if($curr_page == "docs"){
        $prev = "<span class=\"doc_chain_link_$state\">".
                mk_link($curr_title, $curr_page, $curr_subpage).
                "</span>";
        if($state == "next"){
          if($ret != "") { $ret .= " | "; }
          $ret .= $prev." &gt; ";
          break;
        }
      }
    }
  }
  return "<div class=\"doc_chain_link\">$ret</div>";
}

?>
<html><head>
<title><?=$longtitle?></title>
<link rel="stylesheet" type="text/css" href="style.css" />
<script type="text/javascript" src="dropdowntabs.js"> </script>
</head> <body>
<a name="pagetop"></a>
<div class="overallpage">
  <div class="pagebody">
    <div class="headerbar">
      <div class="logobox">
        <?= mk_link("<img src=\"dbtoaster-logo.gif\" width=\"214\" 
                      height=\"100\" alt=\"DBToaster\"/>", "home"); ?></div>
      <div class="topmenu">
        <?php function mk_menu($tag, $page, $subpage = null) {
             return "<li>".mk_link(null, $page, $subpage, "", 
                                   "rel=\"$tag\"")."</li>";
           } ?>
         <div id="bluemenu" class="bluetabs">
            <ul>
               <?= mk_menu("dropmenu0_b", "home") ?>
               <?= mk_menu("dropmenu1_b", "download") ?>
               <?= mk_menu("dropmenu2_b", "docs") ?>
               <?= mk_menu("dropmenu3_b", "home","contact") ?>
           </ul>
         </div>
      </div>
         <div id="dropmenu0_b" class="dropmenudiv_b">
           <?= mk_link(null, "home"); ?>
           <?= mk_link(null, "home", "performance"); ?>
           <?= mk_link(null, "home", "features"); ?>
           <?= mk_link(" - Roadmap", "home", "features", "#roadmap"); ?>
           <?= mk_link(null, "home", "people"); ?>
           <?= mk_link(null, "home", "research"); ?>
         </div>
         <div id="dropmenu1_b" class="dropmenudiv_b">
           <?= mk_link(null, "download", null); ?>
           <?= mk_link("License", "download", null, "#license"); ?>
         </div>
         <div id="dropmenu2_b" class="dropmenudiv_b">
           <?= mk_link("Getting Started", "docs"); ?>
           <?= mk_link(null, "docs", "compiler"); ?>
           <?= mk_link(" - Command-Line Options", "docs", "compiler", 
                                                          "#options"); ?>
           <?= mk_link(" - Supported Languages", "docs", "compiler", 
                                                          "#languages"); ?>
           <?= mk_link(" - Optimization Flags", "docs", "compiler", 
                                                          "#opt_flags"); ?>
           <?= mk_link(null, "docs", "sql"); ?>
           <?= mk_link(null, "docs", "stdlib"); ?>
           <?= mk_link(null, "docs", "adaptors"); ?>
           <?= mk_link(null, "docs", "cpp"); ?>
           <?= mk_link(null, "docs", "scala"); ?>
         </div>
         <div id="dropmenu3_b" class="dropmenudiv_b">
           <?= mk_link("Inquiries", "home", "contact", "#inquiries"); ?>
           <?= mk_link("Mailing List", "home", "contact", "#mailing"); ?>
           <?= mk_link(null, "bugs"); ?>
         </div>
         <script type="text/javascript">
            tabdropdown.init("bluemenu")
         </script>
    </div>
    <hr/>
    <div class="contentwrapper">
      <div class="content">
        <?= chain_link_docs(); ?>
        <?php
          if(isset($pages[$subpage])){ 
        ?><div class="titlebox"><?=$shorttitle?></div><?php
          include($pagepath);
        } else { ?>
          ERROR: The page you have requested does not exist.
        <?php } /* else of isset($pages[$page]) */ ?>
        <?= chain_link_docs(); ?>
      </div><!-- /content -->
    </div><!-- /contentwrapper -->
  </div><!-- /pagebody -->
  <hr/>
  <div class="footer">
  <p>Copyright (c) 2009-2012, The DBToaster Consortium. All rights reserved.</p>
  </div>
</div><!-- /overallpage -->

</body>
</html>
