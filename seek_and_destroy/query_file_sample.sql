#
# ~~ is the equal sign! (because there are equal signs in the queries we cant delimit by that)
# Be sure and end your queries with a semicolon ";"#
#	


# Program Queries
##########################################################


#
# Find MARC that has 856 indicator2=0 and is not cataloged as electronic
#
invalid_ebook_marc~~select id from biblio.record_entry where 
id in (select record from metabib.real_full_rec where tag=$$856$$ and ind2=$$0$$) AND  
marc ~ $$<leader>......[at]$$
and
marc !~ $$tag="008">.......................[oqs]$$
and
marc !~ $$tag="006">......[oqs]$$
and
marc !~ $$<leader>.......p$$;


#
# Find Electronic MARC with physical Items attached
#
electronic_book_with_physical_items_attached~~select id from biblio.record_entry where not deleted and lower(marc) ~ $$<datafield tag="856" ind1="4" ind2="0">$$
and id in
(
select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
)
and 
(
	marc ~ $$tag="008">.......................[oqs]$$
	or
	marc ~ $$tag="006">......[oqs]$$
)
and
(
	marc ~ $$<leader>......[at]$$
)
and
(
	marc ~ $$<leader>.......[acdm]$$
);



#
# Find Electronic Audiobook MARC with physical Items attached
#
electronic_audiobook_with_physical_items_attached~~select id from biblio.record_entry where not deleted and lower(marc) ~ $$<datafield tag="856" ind1="4" ind2="0">$$
and id in
(
select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
)	
and 
(
	marc ~ $$tag="008">.......................[oqs]$$
	or
	marc ~ $$tag="006">......[oqs]$$
)
and
(
	marc ~ $$<leader>......i$$
);



#	non_audiobook_bib_convert_to_audiobook
#----------------------------------------------
# This query should result in all of the bib records that you want to convert to Audio books
# The conversion means: 
# Item Form (008_24, 006_7) will be set to blank " " if its not already
# 007_4 to "f"
# Any 007s that start with "v" will be removed
# Bibs without 007s will have one created

non_audiobook_bib_convert_to_audiobook~~select record
 from seekdestroy.bib_score sbs where 
#Find only Audiobook winners 
 winning_score=$$audioBookScore$$ 
 and 
#Be sure that they do not score as electric at all
electronic=0   
 and 
 (
# Take away those that scored only 1 better than the second place score
	not (winning_score_score>1 and winning_score_distance<2) 
	or
	(
# Or if they did score only 1 better than second place, its ok if they have these qualities
		second_place_score in ($$music_score$$,$$video_score$$)
		and
		(circ_mods ~*$$AudioBooks$$ or circ_mods ~*$$CD$$ )
	)
	or
	(
		opac_icon is null
		and
		second_place_score is null
		and
		LOWER(circ_mods) ~*$$new$$
	)
	or
	(
		opac_icon =$$phonospoken$$
		and
		(second_place_score is null or second_place_score=$$$$)
		and
		(circ_mods ~$$^Books$$ or circ_mods ~*$$,Books$$)
	)
	
 )
and circ_mods !~* $$Refere$$
and record not in
(
	select record from seekdestroy.bib_score where 
	circ_mods =$$Books$$
	and
	opac_icon = $$book$$
)
and record not in
(
	select record from seekdestroy.bib_score where 	
	opac_icon = $$kit$$
)
and not
(	
	(
	circ_mods !~*$$CD$$
	and
	circ_mods !~*$$AudioBooks$$
	and
	circ_mods !~*$$Media$$
	and
	circ_mods !~*$$Kit$$
	and
	circ_mods !~*$$Music$$
	)	
	and
	(
	opac_icon is null 
	and
	circ_mods is null 
	)
	and
	winning_score_score=1	
);

non_audiobook_bib_possible_eaudiobook~~select record
 from seekdestroy.bib_score sbs where 
electronic>0 
and
not
(
opac_icon ~ $$eaudio$$
or
opac_icon ~ $$ebook$$
)
and
winning_score=$$electricScore$$;


#
# Find items that are problably audio related but not attached to audio related bibs
# This is used exclusivly for findItemsCircedAsAudioBooksButAttachedNonAudioBib
#
findItemsCircedAsAudioBooksButAttachedNonAudioBib~~select bre.id,bre.marc,string_agg(ac.barcode,$$,$$) from biblio.record_entry bre, asset.copy ac, asset.call_number acn, asset.copy_location acl where 
acl.id=ac.location and
bre.id=acn.record and
acn.id=ac.call_number and
not acn.deleted and
not ac.deleted and
not bre.deleted and
(
	lower(acn.label) ~* $$cass$$ or
	lower(acn.label) ~* $$aud$$ or
	lower(acn.label) ~* $$disc$$ or
	lower(acn.label) ~* $$mus$$ or
	lower(acn.label) ~* $$ cd$$ or
	lower(acn.label) ~* $$^cd$$ or
	lower(acn.label) ~* $$disk$$
or
	lower(acl.name) ~* $$cas$$ or
	lower(acl.name) ~* $$aud$$ or
	lower(acl.name) ~* $$disc$$ or
	lower(acl.name) ~* $$mus$$ or
	lower(acl.name) ~* $$ cd$$ or
	lower(acl.name) ~* $$^cd$$ or
	lower(acl.name) ~* $$disk$$ 
)
and
ac.circ_modifier in ( $$AudioBooks$$,$$CD$$ ) and
(
(
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$music$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$casaudiobook$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$casmusic$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cassette$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cd$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cdaudiobook$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cdmusic$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$kit$$
)
OR
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) IS NULL
)
group by bre.id,bre.marc;


#
# Find items that are problably not audio related but are attached to audio related bibs
# This is used exclusivly for findItemsNotCircedAsAudioBooksButAttachedAudioBib
#
findItemsNotCircedAsAudioBooksButAttachedAudioBib~~select bre.id,bre.marc,string_agg(ac.barcode,$$,$$) from biblio.record_entry bre, asset.copy ac, asset.call_number acn, asset.copy_location acl where 
bre.marc ~ $$<leader>......i$$ and
acl.id=ac.location and
bre.id=acn.record and
acn.id=ac.call_number and
not acn.deleted and
not ac.deleted and
not bre.deleted and
BRE.ID>0 AND
(
	lower(acn.label) !~* $$cass$$ and
	lower(acn.label) !~* $$aud$$ and
	lower(acn.label) !~* $$disc$$ and
	lower(acn.label) !~* $$mus$$ and
	lower(acn.label) !~* $$ cd$$ and
	lower(acn.label) !~* $$^cd$$ and
	lower(acn.label) !~* $$disk$$
)
and
(
	lower(acl.name) !~* $$cas$$ and
	lower(acl.name) !~* $$aud$$ and
	lower(acl.name) !~* $$disc$$ and
	lower(acl.name) !~* $$mus$$ and
	lower(acl.name) !~* $$ cd$$ and
	lower(acl.name) !~* $$^cd$$ and
	lower(acl.name) !~* $$disk$$ 
)
and
ac.circ_modifier not in ( $$AudioBooks$$,$$CD$$ ) 
group by bre.id,bre.marc;



#
# DVD Bibs convert automatically
#
non_dvd_bib_convert_to_dvd~~select record
 from seekdestroy.bib_score sbs where 

 winning_score~$$video_score$$ 
 and 
electronic=0
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$MARC with video phrases but incomplete marc$$)
 and 
 (
	not (winning_score_score>1 and winning_score_distance<2) 
 )
and record not in
(
	select record from seekdestroy.bib_score where 	
	opac_icon = $$kit$$
)

and record not in
(
	select record from seekdestroy.bib_score sbs where 
	(
	 circ_mods~$$Equipment$$ or
	 circ_mods~$$Media$$ or
	 circ_mods~$$Software$$
	 ) and winning_score_score=1	 
	 and
	 opac_icon~$$software$$
)
and not
(	 
	 circ_mods~$$Magazines$$	 
	 and
	 opac_icon~$$serial$$
	and winning_score_score=1
)
and record not in
(	 
select record from seekdestroy.bib_score sbs where (opac_icon=$$book$$ or opac_icon~$$serial$$) and not (circ_mods~$$EduVid$$ or circ_mods~$$DVD$$ or circ_mods~$$Videos$$) and winning_score_score=1
)
and record not in
(	 
	select record from seekdestroy.bib_score sbs where 
	(circ_mods~$$Reference$$ or circ_mods~$$NewBooks$$ or circ_mods~$$NewBooks$$ or circ_mods~$$Biography$$ or circ_mods~$$BookClub$$ or circ_mods~$$PBKBooks$$ or circ_mods~$$Noncirculating$$ or circ_mods~$$Books$$)
	 and not
	 (circ_mods~$$EduVid$$ or circ_mods~$$DVD$$ or circ_mods~$$Videos$$)
	 and
	 (opac_icon~$$book$$ or length(btrim(opac_icon)) is null)
)
and record not in (select record from seekdestroy.bib_score sbs where circ_mods ~$$Books$$ and opac_icon=$$book$$)
and not circ_mods~$$Kit$$
and not winning_score~$$audioBookScore$$
and winning_score_score!=0
and winning_score_distance>0
and record not in ( select record from seekdestroy.bib_score sbs where opac_icon~$$score$$ and second_place_score~$$music_score$$);


#
# Large Print Bibs convert automatically
#
non_large_print_bib_convert_to_large_print~~
select record
 from seekdestroy.bib_score sbs where record in( select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$MARC with large_print phrases but incomplete marc$$ )
 and winning_score ~ $$largeprint_score$$
 and record not in ( select record from seekdestroy.bib_score  where opac_icon=$$serial$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score  where (lower(call_labels)~$$aud$$ or lower(call_labels)~$$cd$$) and lower(copy_locations)~$$audio$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$AudioBooks$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score  where lower(call_labels)!~$$lp$$ and lower(call_labels)!~$$large$$ and lower(copy_locations)!~$$large$$ and lower(copy_locations)!~$$lp$$ and lower(call_labels)!~$$lg$$ and lower(copy_locations)!~$$lg$$ and lower(call_labels)!~$$sight$$ and lower(copy_locations)!~$$$$ and lower(call_labels)!~$$$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score sbs2  where (select deleted from biblio.record_entry where id= sbs2.record)=$$t$$ and second_place_score !=$$$$ );
 
 
############################################################################################# 
#REPORT QUERIES 
#
#
# Find items that show signs of being large print but are attached to non large print bibs
#
 large_print_items_on_non_large_print_bibs~~select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID),AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
(
ACN.ID IN(SELECT ID FROM ASSET.CALL_NUMBER WHERE (LOWER(LABEL)~$$ lp$$ OR LOWER(LABEL)~$$^lp$$ OR LOWER(LABEL)~$$large$$ OR LOWER(LABEL)~$$lg$$ OR LOWER(LABEL)~$$sight$$) )
OR
ACL.ID IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE (LOWER(NAME)~$$ lp$$ OR LOWER(NAME)~$$^lp$$ OR LOWER(NAME)~$$large$$ OR LOWER(NAME)~$$lg$$ OR LOWER(NAME)~$$sight$$) )
)
AND
BRE.ID IN
(
	SELECT A.ID FROM
	(
	SELECT STRING_AGG(VALUE,$$ $$) "FORMAT",ID from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ GROUP BY ID
	) AS A
	WHERE A."FORMAT"!~$$lpbook$$
	UNION
	SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE ID NOT IN(SELECT ID from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$)
);


#
# Find Items that do not show signs of being large print but are attached to large print bibs
#
 non_large_print_items_on_large_print_bibs~~select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID),AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
(
ACN.ID IN(SELECT ID FROM ASSET.CALL_NUMBER WHERE (LOWER(LABEL)!~$$ lp$$ AND LOWER(LABEL)!~$$^lp$$ AND LOWER(LABEL)!~$$large$$ AND LOWER(LABEL)!~$$lg$$ AND LOWER(LABEL)!~$$sight$$) )
AND
ACL.ID IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE (LOWER(NAME)!~$$ lp$$ AND LOWER(NAME)!~$$^lp$$ AND LOWER(NAME)!~$$large$$ AND LOWER(NAME)!~$$lg$$ AND LOWER(NAME)!~$$sight$$) )
)
AND
BRE.ID IN
(
	SELECT A.ID FROM
	(
	SELECT STRING_AGG(VALUE,$$ $$) "FORMAT",ID from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ GROUP BY ID
	) AS A
	WHERE A."FORMAT"~$$lpbook$$
);



#
# Find Items that show signs of being DVD but are attached to non DVD bibs
#
 DVD_items_on_non_DVD_bibs~~select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID),AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
NOT ACN.DELETED AND
NOT AC.DELETED AND
BRE.ID>0 AND
(
ACN.ID IN(SELECT ID FROM ASSET.CALL_NUMBER WHERE (LOWER(LABEL)~$$ dvd$$ OR LOWER(LABEL)~$$^dvd$$ OR LOWER(LABEL)~$$vhs$$ OR LOWER(LABEL)~$$video$$ OR LOWER(LABEL)~$$movie$$) )
OR
ACL.ID IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE (LOWER(NAME)~$$ dvd$$ OR LOWER(NAME)~$$^dvd$$ OR LOWER(NAME)~$$vhs$$ OR LOWER(NAME)~$$video$$ OR LOWER(NAME)~$$movie$$) )
)
AND
BRE.ID IN
(
	SELECT A.ID FROM
	(
	SELECT STRING_AGG(VALUE,$$ $$) "FORMAT",ID from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ GROUP BY ID
	) AS A
	WHERE A."FORMAT"!~$$dvd$$ AND A."FORMAT"!~$$vhs$$ AND A."FORMAT"!~$$blue$$
	UNION
	SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE ID NOT IN(SELECT ID from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$)
) order by BRE.id;
 
  
#
# Find Items that may not be DVD but are attached to DVD bibs
#
non_DVD_items_on_DVD_bibs~~select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID),AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
NOT ACN.DELETED AND
NOT AC.DELETED AND
BRE.ID>0 AND
(
	lower(acn.label) !~* $$ dvd$$ and
	lower(acn.label) !~* $$^dvd$$ and
	lower(acn.label) !~* $$movie$$ and
	lower(acn.label) !~* $$vhs$$ and
	lower(acn.label) !~* $$video$$
)
and
(
	lower(acl.name) !~* $$ dvd$$ and
	lower(acl.name) !~* $$^dvd$$ and
	lower(acl.name) !~* $$movie$$ and
	lower(acl.name) !~* $$vhs$$ and
	lower(acl.name) !~* $$video$$
)
AND
BRE.ID IN
(
	SELECT A.ID FROM
	(
	SELECT STRING_AGG(VALUE,$$ $$) "FORMAT",ID from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ GROUP BY ID
	) AS A
	WHERE A."FORMAT"~$$dvd$$ or A."FORMAT"~$$blue$$ or A."FORMAT"~$$vhs$$
)
order by BRE.id;
 
#
# Find Items that are probably AUDIOBOOKs but are attached to non Audiobook bibs
#
 Audiobook_items_on_non_Audiobook_bibs~~select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID),AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
NOT ACN.DELETED AND
NOT AC.DELETED AND
BRE.ID>0 AND
(
	lower(acn.label) ~* $$cass$$ or
	lower(acn.label) ~* $$aud$$ or
	lower(acn.label) ~* $$disc$$ or
	lower(acn.label) ~* $$mus$$ or
	lower(acn.label) ~* $$ cd$$ or
	lower(acn.label) ~* $$^cd$$ or
	lower(acn.label) ~* $$disk$$
or
	lower(acl.name) ~* $$cas$$ or
	lower(acl.name) ~* $$aud$$ or
	lower(acl.name) ~* $$disc$$ or
	lower(acl.name) ~* $$mus$$ or
	lower(acl.name) ~* $$ cd$$ or
	lower(acl.name) ~* $$^cd$$ or
	lower(acl.name) ~* $$disk$$ 
)
and
ac.circ_modifier in ( $$AudioBooks$$,$$CD$$ ) and
(
(
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$music$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$casaudiobook$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$casmusic$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cassette$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cd$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cdaudiobook$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cdmusic$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$kit$$
)
OR
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) IS NULL
)
ORDER BY BRE.id,ACN.LABEL
;


#
# Find Items that are probably* NOT AUDIOBOOK but are attached to Audiobook bibs
#
non_Audiobook_items_on_Audiobook_bibs~~select a.id,a.barcode,a.label,a.icon,a.name from
(
select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) as "icon",AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
NOT ACN.DELETED AND
NOT AC.DELETED AND
BRE.ID>0 AND
bre.marc ~ $$<leader>......i$$ and
(
	lower(acn.label) !~* $$cass$$ and
	lower(acn.label) !~* $$aud$$ and
	lower(acn.label) !~* $$disc$$ and
	lower(acn.label) !~* $$mus$$ and
	lower(acn.label) !~* $$ cd$$ and
	lower(acn.label) !~* $$^cd$$ and
	lower(acn.label) !~* $$disk$$
)
and
(
	lower(acl.name) !~* $$cas$$ and
	lower(acl.name) !~* $$aud$$ and
	lower(acl.name) !~* $$disc$$ and
	lower(acl.name) !~* $$mus$$ and
	lower(acl.name) !~* $$ cd$$ and
	lower(acl.name) !~* $$^cd$$ and
	lower(acl.name) !~* $$disk$$ 
)
) as a
#This takes super duper long, so lets not and say we did
-- where
-- (
-- 	a."icon"~ $$music$$ or
-- 	a."icon"~ $$kit$$ or
-- 	a."icon"~ $$casaudiobook$$ or
-- 	a."icon"~ $$casmusic$$ or
-- 	a."icon"~ $$cassette$$ or
-- 	a."icon"~ $$cd$$ or
-- 	a."icon"~ $$cdaudiobook$$ or
-- 	a."icon"~ $$cdmusic$$ or
-- 	a."icon" is null
-- )
order by a.id,a.label;


 
#
# Find Items that are probably* NOT AUDIOBOOK but are attached to Audiobook bibs
# and (union)
# Find Items that are probably AUDIOBOOKs but are attached to non Audiobook bibs

questionable_audiobook_bib_to_item~~select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID),AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
NOT ACN.DELETED AND
NOT AC.DELETED AND
BRE.ID>0 AND
(
	lower(acn.label) ~* $$cass$$ or
	lower(acn.label) ~* $$aud$$ or
	lower(acn.label) ~* $$disc$$ or
	lower(acn.label) ~* $$mus$$ or
	lower(acn.label) ~* $$ cd$$ or
	lower(acn.label) ~* $$^cd$$ or
	lower(acn.label) ~* $$disk$$
or
	lower(acl.name) ~* $$cas$$ or
	lower(acl.name) ~* $$aud$$ or
	lower(acl.name) ~* $$disc$$ or
	lower(acl.name) ~* $$mus$$ or
	lower(acl.name) ~* $$ cd$$ or
	lower(acl.name) ~* $$^cd$$ or
	lower(acl.name) ~* $$disk$$ 
)
and
ac.circ_modifier in ( $$AudioBooks$$,$$CD$$ ) and
(
(
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$music$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$casaudiobook$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$casmusic$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cassette$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cd$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cdaudiobook$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$cdmusic$$ and
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$kit$$
)
OR
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) IS NULL
)
UNION
select a.id,a.barcode,a.label,a.icon,a.name from
(
select BRE.id,AC.BARCODE,ACN.LABEL,(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) as "icon",AOU.NAME
from biblio.record_entry BRE, ASSET.COPY AC, ACTOR.ORG_UNIT AOU,ASSET.CALL_NUMBER ACN,ASSET.COPY_LOCATION ACL where 
AOU.ID=AC.CIRC_LIB AND
BRE.ID=ACN.RECORD AND
ACN.ID=AC.CALL_NUMBER AND
ACL.ID=AC.LOCATION AND
NOT ACN.DELETED AND
NOT AC.DELETED AND
BRE.ID>0 AND
bre.marc ~ $$<leader>......i$$ and
(
	lower(acn.label) !~* $$cass$$ and
	lower(acn.label) !~* $$aud$$ and
	lower(acn.label) !~* $$disc$$ and
	lower(acn.label) !~* $$mus$$ and
	lower(acn.label) !~* $$ cd$$ and
	lower(acn.label) !~* $$^cd$$ and
	lower(acn.label) !~* $$disk$$
)
and
(
	lower(acl.name) !~* $$cas$$ and
	lower(acl.name) !~* $$aud$$ and
	lower(acl.name) !~* $$disc$$ and
	lower(acl.name) !~* $$mus$$ and
	lower(acl.name) !~* $$ cd$$ and
	lower(acl.name) !~* $$^cd$$ and
	lower(acl.name) !~* $$disk$$ 
)
) as a
order by ID
;
