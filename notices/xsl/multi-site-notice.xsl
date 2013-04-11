
<xsl:stylesheet version = '1.0'
     xmlns:fo="http://www.w3.org/1999/XSL/Format"
     xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml"/>

  <xsl:param name="gendate">
  </xsl:param>
  <xsl:param name="lid"/>

  <xsl:template match="file">
    <xsl:variable name="locname" select="$lid" />
    <fo:root>
      <fo:layout-master-set>
        <fo:simple-page-master master-name="late-notice">
          <fo:region-body margin="25mm"/>
        </fo:simple-page-master>
      </fo:layout-master-set>
      <xsl:apply-templates/>    
    </fo:root>
  </xsl:template>

  <xsl:template match="notice">
    <xsl:choose>
      <xsl:when test="location/shortname=$lid">
        <xsl:call-template name="notice_template"/>
      </xsl:when>
      <xsl:when test="location/name[contains(text(), $lid)]">
        <xsl:call-template name="notice_template"/>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="notice_template">
    <xsl:variable name="name1" select="patron/first_given_name" />
    <xsl:variable name="name2" select="patron/family_name" />
    <xsl:variable name="wholename" select="concat($name1,' ',$name2)"/>
    <xsl:variable name="citystatezip" select="concat(patron/addr_city, ' ', patron/addr_state, ' ', patron/addr_post_code)"/>
    <!-- find longest part of address -->
    <xsl:variable name="name-length" select="string-length($wholename)" />
    <xsl:variable name="s1-length" select="string-length(patron/addr_street1)"/>
    <xsl:variable name="s2-length" select="string-length(patron/addr_street2)"/>
    <xsl:variable name="csz-length" select="string-length($citystatezip)"/>
    <xsl:variable name="l1">
      <xsl:choose>
        <xsl:when test="$name-length &gt; $s1-length">
          <xsl:value-of select="$name-length"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$s1-length"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="l2">
      <xsl:choose>
        <xsl:when test="$s2-length &gt; $l1">
          <xsl:value-of select="$s2-length"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$l1"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="longest">
      <xsl:choose>
        <xsl:when test="$csz-length &gt; $l2">
          <xsl:value-of select="$csz-length"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$l2"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="addr-rmargin" select="163 - ($longest * 4) - 1" />

    <fo:page-sequence master-reference="late-notice">
      <fo:flow flow-name="xsl-region-body" font="12pt Helvetica">

        <!-- ##### ADDRESS BLOCK ########################################## -->
        <xsl:element name="fo:block">
          <xsl:attribute name="margin-left">4mm</xsl:attribute>
          <xsl:attribute name="margin-top">38mm</xsl:attribute>
          <xsl:attribute name="margin-bottom"><xsl:choose><xsl:when test="not(patron/addr_street2='')">15mm</xsl:when><xsl:otherwise>20mm</xsl:otherwise></xsl:choose></xsl:attribute>
          <xsl:attribute name="margin-right"><xsl:value-of select="$addr-rmargin"/>mm</xsl:attribute>
          <xsl:attribute name="padding">2mm</xsl:attribute>
          <xsl:attribute name="text-transform">uppercase</xsl:attribute>
          <xsl:attribute name="font-weight">bold</xsl:attribute>
          <xsl:attribute name="background">#cccccc</xsl:attribute>
          <fo:block><xsl:value-of select="$wholename"/></fo:block>
          <fo:block><xsl:value-of select="patron/addr_street1"/></fo:block>
          <xsl:if test="not(patron/addr_street2='')">
            <fo:block><xsl:value-of select="patron/addr_street2"/></fo:block>
          </xsl:if>
          <fo:block><xsl:value-of select="$citystatezip"/></fo:block>
        </xsl:element>

        <!-- ##### SALUTATION ############################################### -->
        <fo:block text-align="right">
          <xsl:value-of select="$gendate"/>
        </fo:block>
        <fo:block>
          Dear <xsl:value-of select="$wholename"/>:
        </fo:block>
        <fo:block margin-top="5mm">
          Our records indicate 
          <xsl:choose>
            <xsl:when test="count(item)&gt;'1'">
              these items are
            </xsl:when>
            <xsl:otherwise>
              this item is
            </xsl:otherwise>
          </xsl:choose>
          <xsl:value-of select="@notify_interval"/>
          overdue:
        </fo:block>

        <!-- ##### ITEMS TABLE ############################################ -->
        <xsl:for-each select="item">
          <?dbfo-need height="2in" ?>
          <fo:table margin-top="5mm" margin-left="2mm" table-layout="fixed"
                    width="100%">
            <fo:table-body>
              <fo:table-row>
                <fo:table-cell text-transform="capitalize" font-style="italic"
                               border-left="1pt solid black">
                  <fo:block>
		 	 <xsl:value-of select="title" /> 
			 <xsl:text>, by </xsl:text>
			<xsl:value-of select="author" />
		</fo:block>	
		</fo:table-cell>
	     </fo:table-row>
            </fo:table-body>
          </fo:table>
          <fo:table margin-bottom="5mm" margin-left="2mm" table-layout="fixed"
                    width="100%" font-size="10pt">
            <fo:table-column column-width="32mm" />
            <fo:table-column column-width="200mm" />
            <fo:table-body>
              <fo:table-row>
                <fo:table-cell border-left="1pt solid black">
                  <fo:block>Due Date</fo:block>
                </fo:table-cell>
                <fo:table-cell font-family="Courier">
                  <fo:block>
                    <xsl:value-of select="due_date" />
                  </fo:block>
                </fo:table-cell>
              </fo:table-row>
              <fo:table-row>
                <fo:table-cell border-left="1pt solid black">
                  <fo:block>Call#</fo:block>
                </fo:table-cell>
                <fo:table-cell font-family="Courier">
                  <fo:block>
                    <xsl:value-of select="call_number" />
                  </fo:block>
                </fo:table-cell>
              </fo:table-row>
              <fo:table-row>
                <fo:table-cell border-left="1pt solid black">
                  <fo:block>Barcode</fo:block>
                </fo:table-cell>
                <fo:table-cell font-family="Courier">
                  <fo:block>
                    <xsl:value-of select="barcode" />
                  </fo:block>
                </fo:table-cell>
              </fo:table-row>
            </fo:table-body>
          </fo:table>
        </xsl:for-each>

        <!-- ##### VARIABLE LATENESS MESSAGE ######################### -->
        <xsl:choose>
          <xsl:when test="@notify_interval='21 days'">
            <fo:block>
              This is your final notice of overdue library materials.
              Please return the above items to avoid additional fines
              and fees.  If the items are not returned, your home
              library may levy additional penalties beyond what is
              listed in your account.  Please contact your library for
              more information.  You can access your account through
              the online catalog at this link:
            </fo:block>
          </xsl:when>
          <xsl:otherwise>
            <fo:block>
              If no other patrons have placed holds on the items and 
	      your library account is in good standing, you may be 
	      able to renew them via the online catalog at:
            </fo:block>
          </xsl:otherwise>
        </xsl:choose>

        <!-- ##### STANDARD FOOTER ##################################### -->
        <fo:block margin="3mm" font="10pt Courier">
          http://missourievergreen.org/
        </fo:block>
        <fo:block>
          Contact your library for more information:
        </fo:block>
        <fo:block margin-top="3mm" margin-left="3mm">
          <xsl:value-of select="location/name"/>
        </fo:block>
        <fo:table font-size="10pt" width="100%" table-layout="fixed">
          <fo:table-column column-width="20mm" />
          <fo:table-column column-width="200mm" />
          <fo:table-body>
            <fo:table-row margin-left="3mm">
              <fo:table-cell><fo:block>Address</fo:block></fo:table-cell>
              <fo:table-cell>
                <fo:block>
                  <xsl:value-of select="location/addr_street1"/>
                </fo:block>
                <xsl:if test="not(location/addr_street2='')">
                  <fo:block><xsl:value-of select="location/addr_street2"/></fo:block>
                </xsl:if>
                <fo:block>
                  <xsl:value-of select="location/addr_city"/>, 
                  <xsl:value-of select="location/addr_state"/>           
                  <xsl:value-of select="concat('   ', location/addr_post_code)"/>
                </fo:block>
              </fo:table-cell>
            </fo:table-row>
            <fo:table-row margin-left="3mm">
              <fo:table-cell><fo:block>Phone</fo:block></fo:table-cell>
              <fo:table-cell font-family="Courier">
                <fo:block><xsl:value-of select="location/phone"/></fo:block>
              </fo:table-cell>
            </fo:table-row>
            <fo:table-row margin-left="3mm">
              <fo:table-cell><fo:block>Email</fo:block></fo:table-cell>
              <fo:table-cell font-family="Courier">
                <fo:block>&lt;<xsl:value-of select="location/email"/>&gt;</fo:block>
              </fo:table-cell>
            </fo:table-row>
          </fo:table-body>
        </fo:table>
      </fo:flow>
    </fo:page-sequence>
  </xsl:template>
</xsl:stylesheet>

