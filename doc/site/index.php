<?
$pages = array(
  "home" => array(
    "title" => "About",
    "contents" => array(
      "index" => array(
        "title" => "About",
      ),
      "performance" => array(
        "title" => "Performance",
      ),
      "features" => array(
        "title" => "Features",
        "contents" => array(
          "roadmap" => "Feature Roadmap",
        )
      ),
      "research" => array(
        "title" => "Research",
        "contents" => array(
          "pubs" => "Publications",
          "presentations" => "Presentations"
        )
      ),
      "contact" => array(
        "title" => "Contact",
        "contents" => array(
          "contact" => "Inquiries",
          "mailing" => "Mailing Lists",
        )
      ),
      "people" => array(
        "title" => "The Team",
      )
    ),
  ),
  "download" => array(
    "title" => "Downloads", 
    "contents" => array(
      "index" => array(
        "title" => "Downloads",
        "contents" => array(
          "latest" => "Latest Release",
        )
      ),
    ),
  ),
  "docs" => array(
    "title" => "Documentation",
    "contents" => array(
      "index" => array(
        "title" => "Getting Started"
      ),
      "compiler" => array(
        "title" => "Command-Line Reference",
        "contents" => array(
          "options" => "Command Line Options",
          "languages" => "Supported Languages",
          "opt_flags" => "Optimization Flags",
        )
      ),
      "sql" => array(
        "title" => "DBT-SQL Reference",
        "contents" => array(
          "create" => "CREATE",
          "select" => "SELECT",
          "include" => "INCLUDE",
        )
      ),
      "stdlib" => array(
        "title" => "Standard Functions",
      ),
      "adaptors" => array(
        "title" => "Standard Adaptors"
      ),
      "cpp" => array(
        "title" => "C++ Code Generator",
        "contents" => array(
          "quickstart" => "Quickstart Guide",
          "apiguide"   => "API Reference",
          "codereference" => "Generated Code Reference",
        )
      ),
      "scala" => array(
        "title" => "Scala Code Generator",
        "contents" => array(
          "quickstart" => "Quickstart Guide",
          "apiguide"   => "API Reference",
          "generatedcode" => "Generated Code Reference",
        )
      ),
    )
  ),
);

if(isset($_GET["page"])){
  $page = $_GET["page"];
  if(isset($_GET["subpage"])){
    $subpage = $_GET["subpage"];
  } else {
    $subpage = "index";
  }
} else {
  $page = "home";
  $subpage = "index";
}
if($subpage == "index"){
  $pagepath = "pages/".$page.".php";
} else {
  $pagepath = "pages/".$page."_".$subpage.".php";
}

$longtitle = "DBToaster";
$shorttitle = "DBToaster";
if(($page != "home") && isset($pages[$page]["title"])){
  $shorttitle = $pages[$page]["title"];
  $longtitle .= ": $shorttitle";
}
if(($subpage != "index") && isset($pages[$page]["contents"])
                 && isset($pages[$page]["contents"][$subpage])
                 && isset($pages[$page]["contents"][$subpage]["title"])) {
  $shorttitle = $pages[$page]["contents"][$subpage]["title"];
  $longtitle .= " - $shorttitle";
}

$chapter = 0; $section = 0; $subsection = 0;
function chapter($title){
  global $chapter,$section,$subsection;
  $chapter ++;
  $section = 0;
  $subsection = 0;
  return "<h3>$chapter. $title</h3>";
}
function section($title){
  global $chapter,$section,$subsection;
  $section ++;
  $subsection = 0;
  return "<h4>$chapter.$section. $title</h4>";
}
function subsection($title){
  global $chapter,$section,$subsection;
  $subsection ++;
  return "<h5>$chapter.$section.$subsection. $title</h5>";
}
function mk_link($text, $page, $subpage = null){
  if($subpage == null){
    $subpage = "";
  } else {
    $subpage = "&subpage=$subpage";
  }
  return "<a href=index.php?page=$page$subpage>$text</a>";
}

?>
<html><head>
<title><?=$longtitle?></title>
<link rel="stylesheet" type="text/css" href="style.css" />
</head><body>
<a name="pagetop"></a>
<div class="overallpage">
  <div class="pagebody">
    <div class="headerbar">
      <div class="logobox"><a href="index.php">
        <img src="dbtoaster-logo.gif" width="214" height="100" alt="DBToaster"/>
      </a></div>
      <div class="topmenu">
        <div class="topmenuitem">
          <a href="index.php?page=home">
            About</a></div>
        <div class="topmenuitem">
          <a href="index.php?page=download">
            Downloads</a></div>
        <div class="topmenuitem">
          <a href="index.php?page=docs">
            Documentation</a></div>
        <div class="topmenuitem">
          <a href="bugs/">
            Bug Reports</a></div>
      </div>
    </div>
    <hr/>
    <div class="menu">
      <? if(isset($pages[$page]["contents"])) { 
           $menu = $pages[$page]["contents"]; ?>
        <ul>
        <? foreach($menu as $menu_subpage_stub => $menu_subpage_info){ ?>
          <li class="leftmenuitem_l1">
            <a href="index.php?page=<?=$page?>&subpage=<?=$menu_subpage_stub?>">
              <?=$menu_subpage_info["title"]?></a>
          <? if(isset($menu_subpage_info["contents"])){ ?>
            <ul>
            <? foreach($menu_subpage_info["contents"] as 
                          $menu_anchor_stub => $menu_anchor_title) { ?>
              <li class="leftmenuitem_l2">
                <a href="index.php?page=<?=$page?>&subpage=<?=$menu_subpage_stub?>#<?=$menu_anchor_stub?>">
                  <?=$menu_anchor_title?></a></li>
            <? } ?></ul>
          <? } ?></li>
      <? } ?></ul>
    <? } ?>
    </div>
    <div class="contentwrapper">
      <div class="content">
        <?
          if(isset($pages[$page])&&isset($pages[$page]["contents"][$subpage])){ 
        ?><div class="titlebox"><?=$shorttitle?></div><?
          include($pagepath);
        } else { ?>
          ERROR: The page you have requested does not exist.
     <? } /* else of isset($pages[$page]) */ ?>
      </div><!-- /content -->
    </div><!-- /contentwrapper -->
  </div><!-- /pagebody -->
  <hr/>
  <div class="footer">
  <p>The views and conclusions contained in the software and documentation are those of the authors and should not be interpreted as representing official policies, either expressed or implied, of The DBToaster Consortium.</p>
  
  <p>Copyright (c) 2009-2012, The DBToaster Consortium. All rights reserved.</p>
  </div>
</div><!-- /overallpage -->

</body>
</html>
