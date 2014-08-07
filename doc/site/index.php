<?php
$pages = array(
  "home"             => "Welcome to dbtoaster.org", 
  "home_about"             => "Is DBToaster right for you?", 
    "home_performance" => "Performance",
    "home_features"    => "Features",
    "home_research"    => "For Researchers",
    "home_contact"     => "Contact",
    "home_people"      => "The Team",
  "download"         => (isset($now_building_distro) ? "License" : "Downloads"),
  "docs"             => "Installation",
    "docs_start"     => "Getting Started",
    "docs_architecture"=> "Architecture",
    "docs_compiler"    => "Command-Line Reference",
    "docs_sql"         => "DBToaster SQL Reference",
    "docs_stdlib"      => "DBToaster StdLib Reference",
    "docs_adaptors"    => "DBToaster Adaptors Reference",
    "docs_cpp"         => "C++ Code Generation",
    "docs_scala"       => "Scala Code Generation",
    "docs_java"        => "DBToaster in Java Programs",
    "docs_customadaptors"=> "Custom Adaptors",
  "bugs"             => "Bug Reports",
  "samples"          => "Sample Queries",
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
if(isset($pages[$page])){
  $longtitle = $pages[$page];
}
if(isset($pages[$subpage])){
  $longtitle = $pages[$subpage];
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
<title>DBToaster - <?=$longtitle?></title>
<link rel="stylesheet" type="text/css" href="css/style.css" />
<link rel="stylesheet" type="text/css" href="css/bootstrap.min.css" media="screen" />
<link rel="stylesheet" type="text/css" href="css/bootstrap-theme.min.css" />
<link rel="shortcut icon" href="favicon.ico" type="image/x-icon">
<link rel="icon" href="favicon.ico" type="image/x-icon">
<link href="//netdna.bootstrapcdn.com/font-awesome/4.0.1/css/font-awesome.css" rel="stylesheet">

<?php if(!isset($now_building_distro)) { ?>
  <script type="text/javascript">
  
    var _gaq = _gaq || [];
    _gaq.push(['_setAccount', 'UA-33263438-1']);
    _gaq.push(['_trackPageview']);
  
    (function() {
      var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
      ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
      var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
    })();
  
  </script>
<?php } ?>

</head> <body>
<a name="pagetop"></a>
<div class="overallpage">
  <div class="pagebody">
    <div class="logobox">
        <?= mk_link("<img src=\"dbtoaster-logo.gif\" width=\"214\" 
                    height=\"100\" alt=\"DBToaster\"/>", "home"); ?>
    </div>
    <div class="navbar navbar-default">
      <div class="navbar-header">
        <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
          <span class="icon-bar"></span>
          <span class="icon-bar"></span>
          <span class="icon-bar"></span>
        </button>
      </div>
      <div class="navbar-collapse collapse">
        <ul class="nav navbar-nav">
          <li class="dropdown">
            <a href="#" class="dropdown-toggle" data-toggle="dropdown">About <b class="caret"></b></a>
            <ul class="dropdown-menu">
              <li><?= mk_link(null, "home"); ?></li>
              <li><?= mk_link(null, "home", "about"); ?></li>
              <li><?= mk_link(null, "home", "performance"); ?></li>
              <li><?= mk_link(null, "home", "features"); ?></li>
              <li><?= mk_link('<small><i class="fa fa-caret-square-o-right"></i> Roadmap</small>', "home", "features", "#roadmap"); ?></li>
              <li><?= mk_link(null, "home", "people"); ?></li>
              <li><?= mk_link(null, "home", "research"); ?></li>
            </ul>
          </li>
          <li class="dropdown">
            <?php if(!isset($now_building_distro)) { ?>
            <a href="#" class="dropdown-toggle" data-toggle="dropdown">Downloads <b class="caret"></b></a>
            <?php } else { ?>
            <a href="#" class="dropdown-toggle" data-toggle="dropdown">License <b class="caret"></b></a>
            <?php } ?>
            <ul class="dropdown-menu">
              <?php if(!isset($now_building_distro)) { ?>
              <li><?= mk_link(null, "download", null); ?></li>
              <?php } ?>
              <li><?= mk_link("License", "download", null, "#license"); ?></li>
              <li><?= mk_link("Changelog", "download", null, "#changelog"); ?></li>
            </ul>
          </li>
          <li class="dropdown">
            <a href="#" class="dropdown-toggle" data-toggle="dropdown">Documentation <b class="caret"></b></a>
            <ul class="dropdown-menu">              
              <li><?= mk_link("Installation", "docs"); ?></li>
              <li><?= mk_link(null, "docs", "start"); ?></li>
              <li><?= mk_link(null, "docs", "architecture"); ?></li>
              <li><?= mk_link(null, "docs", "compiler"); ?></li>
              <li><?= mk_link('<small><i class="fa fa-caret-square-o-right"></i> Command-Line Options</small>', "docs", "compiler", 
                                                        "#options"); ?></li>
              <li><?= mk_link('<small><i class="fa fa-caret-square-o-right"></i> Supported Languages</small>', "docs", "compiler", 
                                                        "#languages"); ?></li>
              <li><?= mk_link('<small><i class="fa fa-caret-square-o-right"></i> Optimization Flags</small>', "docs", "compiler", 
                                                        "#opt_flags"); ?></li>
              <li><?= mk_link(null, "docs", "sql"); ?></li>
              <li><?= mk_link(null, "docs", "stdlib"); ?></li>
              <li><?= mk_link(null, "docs", "adaptors"); ?></li>
              <li><?= mk_link(null, "docs", "cpp"); ?></li>
              <li><?= mk_link(null, "docs", "scala"); ?></li>
              <li><?= mk_link(null, "docs", "java"); ?></li>
              <li><?= mk_link(null, "docs", "customadaptors"); ?></li>
            </ul>
          </li>
          <li class="dropdown">
            <a href="#" class="dropdown-toggle" data-toggle="dropdown">Contact <b class="caret"></b></a>
            <ul class="dropdown-menu">
              <li><?= mk_link("Inquiries", "home", "contact", "#inquiries"); ?></li>

              <li><?= mk_link("Mailing List", "home", "contact", "#mailing"); ?></li>

              <li><?= mk_link(null, "bugs"); ?></li>

            </ul>
          </li>
        </ul>
      </div>
    </div>
    <div class="contentwrapper">
      <div class="content">
        <?= chain_link_docs(); ?>
        <?php
          if(isset($pages[$subpage])){ 
        ?><div class="titlebox"><?=($longtitle=="About" ? "DBToaster" : $longtitle)?></div><?php
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
  <p>Copyright (c) 2009-2014, The DBToaster Consortium. All rights reserved.</p>
  </div>
</div><!-- /overallpage -->

<script type="text/javascript" src="js/jquery-2.0.3.min.js"> </script>
<script type="text/javascript" src="js/bootstrap.min.js"> </script>

</body>
</html>
