<?php if(!isset($now_building_distro)) { ?>
<div class="container">
  <section class="btn-wrapper-download">
    <div class="download-btns">
      <span class="download-text"><i class="fa fa-cloud-download">&nbsp;&nbsp;</i>Download DBToaster 2.1</span>
      <div class="linux-btn-wrapper" style="display: none;">
        <div class="linux-bit" style="display: none;"> <!-- -->
          <ul>
            <li><a href="dist/dbtoaster_rhel6.5_x86_64_3346.tgz">&nbsp;&nbsp;&nbsp;RHEL&nbsp;&nbsp;&nbsp;</a></li>
            <li><a href="dist/dbtoaster_ubuntu12.04_x86_64_3346.tgz">&nbsp;Ubuntu&nbsp;</a></li>
          </ul>
        </div>
        <a href="dist/dbtoaster_ubuntu12.04_x86_64_3346.tgz" class="linux-btn" title="Choose your Linux System" ><i class="fa fa-linux"></i></a>
      </div>
      <a href="dist/dbtoaster_darwin_3346.tgz" class="mac-btn" title="Mac OS X Lion (10.6.8)" style="display: none;"><i class="fa fa-apple"></i></a>
      <a href="dist/dbtoaster_cygwin_3346.tgz" class="windows-btn" title="Windows (Cygwin)" style="display: none;"><i class="fa fa-windows"></i></a>
    </div>
    <!--span id="download-info" style="display: none;">Completely free 7-day full trial</span-->
  </section>
  
  <!-- Store Button -->
  <section class="btn-wrapper-shop">
    <div class="store-btn">
      <?= mk_link("Examples", "samples", null); ?>
    </div>
    <span id="shop-info" style="display: none;">Individual license: 49 US$</span> 
  </section>
</div>
<?php } ?>

<p>DBToaster is an SQL-to-native-code compiler. It generates lightweight, specialized, embeddable query engines for applications that require real-time, low-latency data processing and monitoring capabilities.
The DBToaster compiler generates code that can be easily incorporated into any C++ or JVM-based (Java, Scala, ...) project.
</p>

<p>Since 2009, DBToaster has spearheaded the currently ongoing database compilers revolution.
If you are looking for the fastest possible execution of continuous analytical queries, DBToaster is the answer.
DBToaster code is
<?=mk_link("3-6 orders of magnitude", "home", "performance")?> faster
than all other systems known to us.
</p>

<p>It is also the only freely available query compiler (but check out our
<?=mk_link("license",  "download", null, "#license")?> for
restrictions on commercial use).
</p>


<h4>
<p>
<?=mk_link("Learn more about DBToaster and check whether it is right for you", "home", "about")?>
</p>

<p>
Get started quickly with <?= mk_link("these instructions", "docs", "start"); ?>
</p>

<p>Questions? Contact us at dbtoaster@epfl.ch !</p>
</h4>


