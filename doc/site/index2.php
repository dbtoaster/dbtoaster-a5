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
  "bugs" => array(
    "title" => "Bug Reports",
    "contents" => array(
      "index" => array(
        "title" => "Bug Reports"
      )
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
<script type="text/javascript" src="dropdowntabs.js"> </script>
</head> <body>
<a name="pagetop"></a>
<div class="overallpage">
  <div class="pagebody">
    <div class="headerbar">
      <div class="logobox"><a href="index.php">
        <img src="dbtoaster-logo.gif" width="214" height="100" alt="DBToaster"/>
      </a></div>
      <div class="topmenu">
         <div id="bluemenu" class="bluetabs">
            <ul>
               <li><a href="index.php?page=home" rel="dropmenu0_b">About</a></li>
               <li><a href="index.php?page=download" rel="dropmenu1_b">Download</a></li>
               <li><a href="index.php?page=docs" rel="dropmenu2_b">Documentation</a></li>
               <li><a href="index.php?page=home&subpage=contact" rel="dropmenu3_b">Contact</a></li>
           </ul>
         </div>
      </div>
         <div id="dropmenu0_b" class="dropmenudiv_b">
            <a href="index.php?page=home">Home</a>
            <a href="index.php?page=home&subpage=performance">Performance</a>
            <a href="index.php?page=home&subpage=features">Features</a>
            <a href="index.php?page=home&subpage=features#roadmap">Feature Roadmap</a>
            <a href="index.php?page=home&subpage=people">Team</a>
            <a href="index.php?page=home&subpage=research">For researchers</a>
         </div>
         <div id="dropmenu1_b" class="dropmenudiv_b">
            <a href="index.php?page=download">Download</a>
            <a href="index.php?page=download">License</a>
         </div>
         <div id="dropmenu2_b" class="dropmenudiv_b">
            <a href="index.php?page=docs">Getting Started</a>
            <a href="index.php?page=docs&subpage=compiler">Command-Line Reference</a>
            <a href="index.php?page=docs&subpage=compiler#options">- Command-Line Options</a>
            <a href="index.php?page=docs&subpage=compiler#languages">- Supported Languages</a>
            <a href="index.php?page=docs&subpage=compiler#opt_flags">- Optimization Flags</a>
            <a href="index.php?page=docs&subpage=sql">DBT-SQL Reference</a>
            <a href="index.php?page=docs&subpage=stdlib">Standard Functions</a>
            <a href="index.php?page=docs&subpage=adaptors">Standard Adaptors</a>
            <a href="index.php?page=docs&subpage=cpp">C++ Code Generator</a>
            <a href="index.php?page=docs&subpage=scala"Scala Code Generator</a>
         </div>
         <div id="dropmenu3_b" class="dropmenudiv_b">
            <a href="index.php?page=home&subpage=contact#Inquiries">Inquiries</a>
            <a href="index.php?page=home&subpage=contact#Mailing%20Lists">Mailing List</a>
            <a href="index.php?page=bugs">Bug Reports</a>
         </div>
         <script type="text/javascript">
            tabdropdown.init("bluemenu")
         </script>
    </div>
    <hr/>
<!--
    <div class="menu">
      <div class="menutop"></div><div class="menucontent">
        <? if(isset($pages[$page]["contents"])) { 
             $menu = $pages[$page]["contents"]; ?>
          <? foreach($menu as $menu_subpage_stub => $menu_subpage_info){ ?>
            <a href="index.php?page=<?=$page?>&subpage=<?=$menu_subpage_stub?>" class="leftlink">
              <div class="leftmenuitem_l1">
                <?=$menu_subpage_info["title"]?></a></div>
            <? if(isset($menu_subpage_info["contents"])){ ?>
              <? foreach($menu_subpage_info["contents"] as 
                            $menu_anchor_stub => $menu_anchor_title) { ?>
                <a href="index.php?page=<?=$page?>&subpage=<?=$menu_subpage_stub?>#<?=$menu_anchor_stub?>" class="leftlink">
                  <div class="leftmenuitem_l2">
                    <?=$menu_anchor_title?></div></a>
              <? } ?>
            <? } ?>
          <? } ?>
        <? } ?>
      </div>
      <div class="menubottom"></div>
    </div>
-->
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
  <p>Copyright (c) 2009-2012, The DBToaster Consortium. All rights reserved.</p>
  </div>
</div><!-- /overallpage -->

</body>
</html>
