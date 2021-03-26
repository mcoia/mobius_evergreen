#
# ~~ is the equal sign! (because there are equal signs in the queries we cant delimit by that)
# Be sure and end your queries with a semicolon ";"#
#


# Program Queries
##########################################################


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


#
# Electronic Bibs convert automatically
#
non_electronic_bib_convert_to_electronic~~
select record
 from seekdestroy.bib_score sbs where
 winning_score=$$electricScore$$
 and
 opac_icon !=$$eaudio$$
 and
 opac_icon !=$$ebook$$
 and
 opac_icon !=$$evideo$$
 and
 winning_score_score>6
 and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
 ;

#
# Electronic Need Humans
#
non_electronic_bib_not_convert_to_electronic~~
select record
 from seekdestroy.bib_score sbs where
 record not in(
 select record
 from seekdestroy.bib_score sbs where
 winning_score=$$electricScore$$
 and
 opac_icon !=$$eaudio$$
 and
 opac_icon !=$$ebook$$
 and
 opac_icon !=$$evideo$$
 and
 winning_score_score>6
 and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
 )
 and
 winning_score~$$electricScore$$
 and
 winning_score_score<7
 and
 winning_score_score>1
 and
 opac_icon !=$$eaudio$$
 and
 opac_icon !=$$evideo$$
 and
 opac_icon !=$$ebook$$
 and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
;


#   non_audiobook_bib_convert_to_audiobook
#----------------------------------------------
# This query should result in all of the bib records that you want to convert to Audio books
# The conversion means:
# Item Form (008_24, 006_7) will be set to blank " " if its not already
# 007_4 to "f"
# Any 007s that start with "v" will be removed
# Bibs without 007s will have one created

non_audiobook_bib_convert_to_audiobook~~select record
 from seekdestroy.bib_score sbs where

 winning_score=$$audioBookScore$$
and
electronic=0
and record not in(select record from seekdestroy.bib_score where winning_score_score>1 and winning_score_distance<2)
and record not in(select record from seekdestroy.bib_score where opac_icon=$$phonospoken$$ and (second_place_score is null or second_place_score=$$$$) and (circ_mods ~$$^BOOK$$ or circ_mods ~*$$,BOOK$$))
and record not in(select record from seekdestroy.bib_score where circ_mods = $$BOOK$$ and opac_icon = $$book$$)
and record not in(select record from seekdestroy.bib_score where opac_icon = $$kit$$)
and record not in(select record from seekdestroy.bib_score where opac_icon = $$casaudiobook$$)
and record not in(select record from seekdestroy.bib_score where opac_icon ~ $$cas$$ and (lower(call_labels)~$$cas$$ or lower(copy_locations)~$$cas$$ ) )
and record not in(select record from seekdestroy.bib_score where (lower(call_labels)~$$music$$ or lower(copy_locations)~$$music$$ ) )
and record not in(select record from seekdestroy.bib_score where winning_score_score=1 and circ_mods is null and    opac_icon is null   )
and record not in(select record from seekdestroy.bib_score where record_type in ($$p$$,$$o$$))
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$eaudio$$)
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$phono$$)
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$playaway$$)
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$kit$$)
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
;

#
# Audiobook NEEDS HUMANS
#
non_audiobook_bib_not_convert_to_audiobook~~select record from seekdestroy.bib_score sbs where record not in
(
select record
 from seekdestroy.bib_score sbs where

 winning_score=$$audioBookScore$$
and
electronic=0
and record not in(select record from seekdestroy.bib_score where winning_score_score>1 and winning_score_distance<2)
and record not in(select record from seekdestroy.bib_score where opac_icon=$$phonospoken$$ and (second_place_score is null or second_place_score=$$$$) and (circ_mods ~$$^BOOK$$ or circ_mods ~*$$,BOOK$$))
and record not in(select record from seekdestroy.bib_score where circ_mods = $$BOOK$$ and opac_icon = $$book$$)
and record not in(select record from seekdestroy.bib_score where opac_icon = $$kit$$)
and record not in(select record from seekdestroy.bib_score where opac_icon = $$casaudiobook$$)
and record not in(select record from seekdestroy.bib_score where opac_icon ~ $$cas$$ and (lower(call_labels)~$$cas$$ or lower(copy_locations)~$$cas$$ ) )
and record not in(select record from seekdestroy.bib_score where (lower(call_labels)~$$music$$ or lower(copy_locations)~$$music$$ ) )
and record not in(select record from seekdestroy.bib_score where winning_score_score=1 and circ_mods is null and    opac_icon is null   )
and record not in(select record from seekdestroy.bib_score where record_type in ($$p$$,$$o$$))
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$eaudio$$)
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$phono$$)
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$playaway$$)
and record not in(select record from SEEKDESTROY.bib_score WHERE opac_icon ~ $$kit$$)
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
)
and winning_score ~ $$audioBookScore$$
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
and winning_score_score!=0
and record not in(select record from seekdestroy.bib_score where opac_icon ~ $$kit$$ and circ_mods~$$KIT$$ and record_type=$$p$$)
and record not in(select record from seekdestroy.bib_score where opac_icon ~ $$eaudio$$ and audioformat~$$z$$ and record_type=$$i$$)
and record not in(select record from seekdestroy.bib_score where opac_icon ~ $$lpbook$$ and circ_mods~$$BOOK$$ and record_type=$$a$$ )
and record not in(select record from seekdestroy.bib_score where opac_icon ~ $$playaway$$ and circ_mods~$$AUDIOBOOK$$ and record_type=$$i$$ and audioformat in($$z$$,$$u$$) )
;

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
(SELECT STRING_AGG(VALUE,$$ $$) "FORMAT" from METABIB.RECORD_ATTR_FLAT WHERE ATTR=$$icon_format$$ AND ID=BRE.ID GROUP BY ID) !~ $$playaway$$ and
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
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
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
    select record from seekdestroy.bib_score where
    opac_icon = $$map$$
)
and record not in
(
    select record from seekdestroy.bib_score where
     circ_mods!~$$VIDEO$$
)
and record not in
(
    select record from seekdestroy.bib_score where
    (
     circ_mods~$$EQUIPMENT$$ or
     circ_mods~$$PLAYAWAY$$ or
     circ_mods~$$TECHNOLOGY$$ or
     circ_mods~$$SOFTWARE$$
     ) and winning_score_score=1
     and
     opac_icon~$$software$$
)
and record not in
(
     select record from seekdestroy.bib_score where
     (
     circ_mods~$$MAGAZINE$$ or
     circ_mods~$$PERIODICAL$$ or
     circ_mods~$$NEWSPAPER$$
     ) and winning_score_score=1
     and
     opac_icon~$$serial$$
)
and record not in
(
select record from seekdestroy.bib_score where (opac_icon=$$book$$ or opac_icon~$$serial$$) and not (circ_mods~$$VIDEO$$ or circ_mods~$$MUSIC$$) and winning_score_score=1
)
and record not in
(
    select record from seekdestroy.bib_score where
     not
     (circ_mods~$$VIDEO$$ or circ_mods~$$TECHNOLOGY$$ or circ_mods~$$PLAYAWAY$$)
     and
     (opac_icon~$$book$$ or length(btrim(opac_icon)) is null)
)
and record not in (select record from seekdestroy.bib_score where circ_mods ~$$BOOK$$ and opac_icon=$$book$$)
and record not in ( select record from seekdestroy.bib_score where opac_icon~$$score$$ and second_place_score~$$music_score$$)
and record not in ( select record from seekdestroy.bib_score where opac_icon~$$music$$ and second_place_score~$$music$$ and (copy_locations~*$$cd$$ or copy_locations~*$$music$$ or call_labels~*$$cd$$ or call_labels~*$$music$$))
and record not in ( select record from seekdestroy.bib_score where opac_icon~$$dvd$$)
and not circ_mods~$$KIT$$
and not winning_score~$$audioBookScore$$
and winning_score_score!=0
and winning_score_distance>0
;


non_dvd_bib_not_convert_to_dvd~~select record from seekdestroy.bib_score where record not in
(
select record
 from seekdestroy.bib_score sbs where

 winning_score~$$video_score$$
 and
electronic=0
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
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
    select record from seekdestroy.bib_score where
    opac_icon = $$map$$
)
and record not in
(
    select record from seekdestroy.bib_score where
     circ_mods!~$$VIDEO$$
)
and record not in
(
    select record from seekdestroy.bib_score where
    (
     circ_mods~$$EQUIPMENT$$ or
     circ_mods~$$PLAYAWAY$$ or
     circ_mods~$$TECHNOLOGY$$ or
     circ_mods~$$GAME$$ or
     circ_mods~$$SOFTWARE$$
     ) and winning_score_score=1
     and
     opac_icon~$$software$$
)
and record not in
(
     select record from seekdestroy.bib_score where
     (
     circ_mods~$$MAGAZINE$$ or
     circ_mods~$$PERIODICAL$$ or
     circ_mods~$$NEWSPAPER$$
     ) and winning_score_score=1
     and
     opac_icon~$$serial$$
)
and record not in
(
select record from seekdestroy.bib_score where (opac_icon=$$book$$ or opac_icon~$$serial$$) and not (circ_mods~$$VIDEO$$ or circ_mods~$$MUSIC$$) and winning_score_score=1
)
and record not in
(
    select record from seekdestroy.bib_score where
     not
     (circ_mods~$$VIDEO$$ or circ_mods~$$TECHNOLOGY$$ or circ_mods~$$PLAYAWAY$$)
     and
     (opac_icon~$$book$$ or length(btrim(opac_icon)) = 0 or opac_icon is null)
)
and record not in (select record from seekdestroy.bib_score where circ_mods ~$$BOOK$$ and opac_icon=$$book$$)
and record not in ( select record from seekdestroy.bib_score where opac_icon~$$score$$ and second_place_score~$$music_score$$)
and record not in ( select record from seekdestroy.bib_score where opac_icon~$$music$$ and second_place_score~$$music$$ and (copy_locations~*$$cd$$ or copy_locations~*$$music$$ or call_labels~*$$cd$$ or call_labels~*$$music$$))
and record not in ( select record from seekdestroy.bib_score where opac_icon~$$dvd$$)
and not circ_mods~$$KIT$$
and not winning_score~$$audioBookScore$$
and winning_score_score!=0
and winning_score_distance>0
)
and winning_score~$$video_score$$
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
and winning_score_score!=0
and winning_score_distance!=0
;


#
# Large Print Bibs convert automatically
#
non_large_print_bib_convert_to_large_print~~
select record
 from seekdestroy.bib_score sbs where record in( select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$ )
 and winning_score ~ $$largeprint_score$$
 and winning_score_score!=0
 and record not in ( select record from seekdestroy.bib_score  where opac_icon=$$serial$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score  where (lower(call_labels)~$$aud$$ or lower(call_labels)~$$cd$$) and lower(copy_locations)~$$audio$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$AUDIOBOOK$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$MUSIC$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$SOFTWARE$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$GAME$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$EQUIPMENT$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$MICROFORM$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$VIDEO$$)
 and record not in ( select record from seekdestroy.bib_score  where opac_icon~$$ebook$$)
 and record not in ( select record from seekdestroy.bib_score  where lower(call_labels)!~$$lp$$ and lower(call_labels)!~$$large$$ and lower(copy_locations)!~$$large$$ and lower(copy_locations)!~$$lp$$ and lower(call_labels)!~$$lg$$ and lower(copy_locations)!~$$lg$$ and lower(call_labels)!~$$sight$$  and btrim(copy_locations)!=$$$$ and btrim(call_labels)!=$$$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score sbs2  where (select deleted from biblio.record_entry where id= sbs2.record)=$$t$$ and second_place_score !=$$$$ )
 ;

#
# Large Print NEEDS HUMANS
#
non_large_print_bib_not_convert_to_large_print~~
select record
 from seekdestroy.bib_score
 where record not in(
select record
 from seekdestroy.bib_score sbs where record in( select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$ )
 and winning_score ~ $$largeprint_score$$
 and winning_score_score!=0
 and record not in ( select record from seekdestroy.bib_score  where opac_icon=$$serial$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score  where (lower(call_labels)~$$aud$$ or lower(call_labels)~$$cd$$) and lower(copy_locations)~$$audio$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$AUDIOBOOK$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$MUSIC$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$SOFTWARE$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$GAME$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$EQUIPMENT$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$MICROFORM$$)
 and record not in ( select record from seekdestroy.bib_score  where circ_mods~$$VIDEO$$)
 and record not in ( select record from seekdestroy.bib_score  where opac_icon~$$ebook$$)
 and record not in ( select record from seekdestroy.bib_score  where lower(call_labels)!~$$lp$$ and lower(call_labels)!~$$large$$ and lower(copy_locations)!~$$large$$ and lower(copy_locations)!~$$lp$$ and lower(call_labels)!~$$lg$$ and lower(copy_locations)!~$$lg$$ and lower(call_labels)!~$$sight$$  and btrim(copy_locations)!=$$$$ and btrim(call_labels)!=$$$$ and winning_score_score=1)
 and record not in ( select record from seekdestroy.bib_score sbs2  where (select deleted from biblio.record_entry where id= sbs2.record)=$$t$$ and second_place_score !=$$$$ )
 )
 and winning_score ~ $$largeprint_score$$
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
and winning_score_score!=0
 ;

#
# Music Bibs convert automatically
#
non_music_bib_convert_to_music~~
select record
 from seekdestroy.bib_score
 where
 record in (select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
 and record not in ( select record from seekdestroy.bib_score where opac_icon~$$music$$ or opac_icon~$$score$$ or opac_icon~$$phono$$ or opac_icon~$$kit$$ or opac_icon~$$audiobook$$)
 and record not in ( select record from seekdestroy.bib_score where trim(BOTH $$,$$ from circ_mods) != $$MUSIC$$)
 and record not in ( select record from seekdestroy.bib_score where btrim(call_labels)=$$$$ and winning_score_score<5)
 and record not in ( select record from seekdestroy.bib_score where call_labels!~*$$music$$ and copy_locations!~*$$music$$)
 and winning_score = $$music_score$$
 and winning_score_score!=0
 ;

#
# Music NEEDS HUMANS
#
non_music_bib_not_convert_to_music~~
select record
 from seekdestroy.bib_score
 where record not in(
select record
 from seekdestroy.bib_score
 where
 record in (select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
 and record not in ( select record from seekdestroy.bib_score where opac_icon~$$music$$ or opac_icon~$$score$$ or opac_icon~$$phono$$ or opac_icon~$$kit$$ or opac_icon~$$audiobook$$)
 and record not in ( select record from seekdestroy.bib_score where trim(BOTH $$,$$ from circ_mods) != $$MUSIC$$)
 and record not in ( select record from seekdestroy.bib_score where btrim(call_labels)=$$$$ and winning_score_score<5)
 and record not in ( select record from seekdestroy.bib_score where call_labels!~*$$music$$ and copy_locations!~*$$music$$)
 and winning_score = $$music_score$$
 and winning_score_score!=0
 )
 and record not in ( select record from seekdestroy.bib_score where opac_icon~$$music$$)
 and record not in ( select record from seekdestroy.bib_score where opac_icon~$$score$$)
 and record not in ( select record from seekdestroy.bib_score where opac_icon~$$phono$$)
 and record not in ( select record from seekdestroy.bib_score where circ_mods~$$AUDIOBOOK$$)
 and record not in ( select record from seekdestroy.bib_score where opac_icon~$$kit$$)
 and record not in ( select record from seekdestroy.bib_score where opac_icon~$$audiobook$$)
 and winning_score = $$music_score$$
and record in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$)
and winning_score_score>2
;


# Program Queries - Format search phrase
##########################################################



#
# Electronic Bibs search phrases
#

electronic_search_phrase~~select id,marc from biblio.record_entry where
    not
    (
        (
        marc ~ $$tag="008">.......................[oqs]$$ or
        marc ~ $$tag="006">......[oqs]$$
        )
        and
        marc ~ $$<leader>.......p$$
    )
    AND
    lower(marc) ~* $$$phrase$$
    and
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    );

#
# Additional Electronic search - Find MARC that has 856 indicator2=0 and is not cataloged as electronic
#

electronic_additional_search~~select id,marc from biblio.record_entry where
    id in (select record from metabib.real_full_rec where tag=$$856$$ and ind2=$$0$$) AND
    marc ~ $$<leader>......[at]$$
    and
    not
    (
        (
        marc ~ $$tag="008">.......................[oqs]$$ or
        marc ~ $$tag="006">......[oqs]$$
        )
        and
        marc ~ $$<leader>.......p$$
    )
    and
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    );



#
# Audiobook Bibs search phrases
#

audiobook_search_phrase~~select id,marc from biblio.record_entry where
    (
    marc !~ $$tag="007">s..[fl]$$
    OR
    marc !~ $$<leader>......[i]$$
    )
    AND
    lower(marc) ~* $$$phrase$$
    AND
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    );

audiobook_additional_search~~select id,marc from biblio.record_entry where
    (
    marc !~ $$tag="007">s..[fl]$$
    OR
    marc !~ $$<leader>......[i]$$
    )
    AND
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    )
    AND
    id in
    ( select record from asset.call_number where id in(select call_number from asset.copy where circ_modifier=$$AudioBooks$$))
    ;


#
# DVD/video search phrases
#

dvd_search_phrase~~select id,marc from biblio.record_entry where
    (
        marc !~ $$tag="007">v...[vbs]$$
    )
    AND
    lower(marc) ~* $$$phrase$$
    AND
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    );

dvd_additional_search~~select id,marc from biblio.record_entry where
    (
    marc !~ $$tag="007">v...[vbs]$$
    )
    AND
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    )
    AND
    id in
    ( select record from asset.call_number where id in(select call_number from asset.copy where circ_modifier in($$VIDEO$$)))
    ;



#
# Large Print search phrases
#

largeprint_search_phrase~~select id,marc from biblio.record_entry where
    (
        marc !~ $$<leader>......[atd]$$
    OR
        (
        marc !~ $$tag="008">.......................[d]$$
        and
        marc !~ $$tag="006">......[d]$$
        )
    OR
        marc !~ $$<leader>.......[acdm]$$
    )
    AND
    lower(marc) ~* $$$phrase$$
    AND
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    );


#
# Music search phrases
#

music_search_phrase~~select id,marc from biblio.record_entry where
    (
    marc !~ $$tag="007">s..[lf]$$
    OR
    marc !~ $$<leader>......[j]$$
    )
    AND
    lower(marc) ~* $$$phrase$$
    AND
    lower(marc) !~* $$non music$$
    AND
    lower(marc) !~* $$non-music$$
    AND
    lower(marc) !~* $$talking books$$
    AND
    lower(marc) !~* $$recorded books$$
    AND
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    );


music_additional_search~~select id,marc from biblio.record_entry where
    (
    marc !~ $$tag="007">s..[lf]$$
    OR
    marc !~ $$<leader>......[j]$$
    )
    AND
    id not in
    (
    select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=$$$problemphrase$$
    )
    AND
    id in
    (
    select record from asset.call_number where id in(select call_number from asset.copy where circ_modifier=$$MUSIC$$)
    )
    ;


#############################################################################################
#REPORT QUERIES
#


#
# Possible Electronic
#

possible_electronic~~SELECT
record
FROM seekdestroy.bib_score sbs
WHERE
electronic>0
AND NOT opac_icon ~ $$eaudio$$
AND NOT opac_icon ~ $$ebook$$
AND NOT opac_icon ~ $$evideo$$
AND winning_score~$$electricScore$$;


#
# Find items that show signs of being large print but are attached to non large print bibs
#
questionable_large_print~~SELECT * from (SELECT
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is large print, bib is not$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$lpbook$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        acn_labels.label~*$$ lp$$ OR
        acn_labels.label~*$$^lp$$ OR
        acn_labels.label~*$$large$$ OR
        acn_labels.label~*$$lg$$ OR
        acn_labels.label~*$$sight$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
        acl_names.name~*$$ lp$$ OR
        acl_names.name~*$$^lp$$ OR
        acl_names.name~*$$large$$ OR
        acl_names.name~*$$lg$$ OR
        acl_names.name~*$$sight$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NULL OR format_icons.id IS NULL ) AND
bre.id>0 AND bre.id < 10000 AND
(
    acl_names.id is not null
    OR
    acn_labels.id is not null
)
GROUP BY 1,2,3,5,6,7,8,9

UNION ALL

select
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is not large print, bib is$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$lpbook$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        acn_labels.label~*$$ lp$$ OR
        acn_labels.label~*$$^lp$$ OR
        acn_labels.label~*$$large$$ OR
        acn_labels.label~*$$lg$$ OR
        acn_labels.label~*$$sight$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
        acl_names.name~*$$ lp$$ OR
        acl_names.name~*$$^lp$$ OR
        acl_names.name~*$$large$$ OR
        acl_names.name~*$$lg$$ OR
        acl_names.name~*$$sight$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NOT NULL AND format_icons.id IS NOT NULL ) AND
bre.id>0 AND bre.id < 10000 AND
(
    acl_names.id IS NULL
    AND
    acn_labels.id IS NULL
)

GROUP BY 1,2,3,5,6,7,8,9
) AS a
ORDER BY lower(a."Branch");

#
# Find Questionable video format mismatches
#
questionable_video_bib_to_item~~SELECT * from (SELECT
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is video, bib is not$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$dvd$$ OR format_icons.value ~* $$blu$$ OR format_icons.value ~* $$vhs$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        acn_labels.label~*$$ dvd$$ OR
        acn_labels.label~*$$^dvd$$ OR
        acn_labels.label~*$$vhs$$ OR
        acn_labels.label~*$$video$$ OR
        acn_labels.label~*$$movie$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
        acl_names.name~*$$ dvd$$ OR
        acl_names.name~*$$^dvd$$ OR
        acl_names.name~*$$vhs$$ OR
        acl_names.name~*$$video$$ OR
        acl_names.name~*$$movie$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NULL OR format_icons.id IS NULL ) AND
bre.id>0 AND bre.id < 10000 AND
(
    acl_names.id IS NOT NULL
    OR
    acn_labels.id IS NOT NULL
    OR
    ac.circ_modifier ~* $$video$$
)
GROUP BY 1,2,3,5,6,7,8,9

UNION ALL

select
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is not video, bib is$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$dvd$$ OR format_icons.value ~* $$blu$$ OR format_icons.value ~* $$vhs$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        acn_labels.label~*$$ dvd$$ OR
        acn_labels.label~*$$^dvd$$ OR
        acn_labels.label~*$$vhs$$ OR
        acn_labels.label~*$$video$$ OR
        acn_labels.label~*$$movie$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
        acl_names.name~*$$ dvd$$ OR
        acl_names.name~*$$^dvd$$ OR
        acl_names.name~*$$vhs$$ OR
        acl_names.name~*$$video$$ OR
        acl_names.name~*$$movie$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NOT NULL AND format_icons.id IS NOT NULL ) AND
bre.id>0 AND bre.id < 10000 AND
(
    acl_names.id IS NULL
    AND
    acn_labels.id IS NULL
    AND
    ac.circ_modifier !~* $$video$$
)

GROUP BY 1,2,3,5,6,7,8,9
) AS a
ORDER BY lower(a."Branch");



#
# Find Questionable music format mismatches
#
questionable_music_bib_to_item~~SELECT * from (SELECT
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is music, bib is not$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$music$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        (
        acn_labels.label~*$$music$$ OR
        acn_labels.label~*$$^folk$$ OR
        acn_labels.label~*$$ folk$$ OR
        acn_labels.label~*$$classical$$ OR
        acn_labels.label~*$$listening$$ OR
        acn_labels.label~*$$[rock|classic|gospel|holiday]\scd$$ OR
        acn_labels.label~*$$sound$$ OR
        acn_labels.label~*$$casse$$
        ) AND
        acn_labels.label!~*$$folktale$$ AND
        acn_labels.label!~*$$spoken$$ AND
        acn_labels.label!~*$$audio cd$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
        acl_names.name~*$$music$$ OR
        acl_names.name~*$$singalong$$ OR
        acl_names.name~*$$readalong$$ OR
        acl_names.name~*$$casse$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NULL OR format_icons.id IS NULL ) AND
bre.id>0 AND bre.id < 100000 AND
(
    acl_names.id is not null
    OR
    acn_labels.id is not null
    OR
    ac.circ_modifier~$$music$$
)
GROUP BY 1,2,3,5,6,7,8,9

UNION ALL

select
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is not music, bib is$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$music$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        (
        acn_labels.label~*$$music$$ OR
        acn_labels.label~*$$^folk$$ OR
        acn_labels.label~*$$ folk$$ OR
        acn_labels.label~*$$classical$$ OR
        acn_labels.label~*$$listening$$ OR
        acn_labels.label~*$$[rock|classic|gospel|holiday]\scd$$ OR
        acn_labels.label~*$$sound$$ OR
        acn_labels.label~*$$casse$$
        ) AND
        acn_labels.label!~*$$folktale$$ AND
        acn_labels.label!~*$$spoken$$ AND
        acn_labels.label!~*$$audio cd$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
        acl_names.name~*$$music$$ OR
        acl_names.name~*$$singalong$$ OR
        acl_names.name~*$$readalong$$ OR
        acl_names.name~*$$casse$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NOT NULL AND format_icons.id IS NOT NULL ) AND
bre.id>0 AND bre.id < 100000 AND
(
    acl_names.id IS NULL
    AND
    acn_labels.id IS NULL
    AND
    ac.circ_modifier!~*$$music$$
)

GROUP BY 1,2,3,5,6,7,8,9
) AS a
ORDER BY lower(a."Branch");

#
# Find Items that are probably* AUDIOBOOK but are attached to non Audiobook bibs
# and (union)
# Find Items that are probably not AUDIOBOOKs but are attached to Audiobook bibs
questionable_audiobook_bib_to_item~~SELECT * from (SELECT
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is audiobook, bib is not$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$audiobook$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        acn_labels.label~*$$cass$$ OR
        acn_labels.label~*$$audio$$ OR
        acn_labels.label~*$$dis[ck]$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
	    acl_names.name~*$$cass$$ OR
        acl_names.name~*$$audio$$ OR
        acl_names.name~*$$dis[ck]$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NULL OR format_icons.id IS NULL ) AND
bre.id>0 AND bre.id < 100000 AND
(
    acl_names.id is not null
    OR
    acn_labels.id is not null
    OR
    ac.circ_modifier~$$audiobook$$
)
GROUP BY 1,2,3,5,6,7,8,9

UNION ALL

select
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
AOU.NAME AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item is not audiobook, bib is$$ AS "Issue",
ac.id AS "copyid"
FROM
biblio.record_entry bre
LEFT JOIN metabib.record_attr_flat format_icons ON (bre.id=format_icons.id and format_icons.attr=$$icon_format$$ and (format_icons.value ~* $$audiobook$$))
LEFT JOIN metabib.record_attr_flat mraf ON (mraf.id=bre.id and mraf.attr=$$icon_format$$)
JOIN asset.call_number acn ON (bre.id=acn.record and not acn.deleted)
JOIN asset.copy ac ON (ac.call_number=acn.id and not ac.deleted)
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!)
JOIN actor.org_unit aou ON (aou.id=ac.circ_lib)
JOIN asset.copy_location acl ON (acl.id=ac.location and not acl.deleted)
LEFT JOIN asset.call_number acn_labels ON
(
    acn.id=acn_labels.id and
    (
        acn_labels.label~*$$cass$$ OR
        acn_labels.label~*$$audio$$ OR
        acn_labels.label~*$$dis[ck]$$
    )
)
LEFT JOIN asset.copy_location acl_names ON
(
    acl.id=acl_names.id and
    (
        acl_names.name~*$$cass$$ OR
        acl_names.name~*$$audio$$ OR
        acl_names.name~*$$dis[ck]$$
    )
)
WHERE
sil.report IS NULL AND
( mraf.id IS NOT NULL AND format_icons.id IS NOT NULL ) AND
bre.id>0 AND bre.id < 100000 AND
(
    acl_names.id IS NULL
    AND
    acn_labels.id IS NULL
    AND
    ac.circ_modifier!~*$$audiobook$$
)

GROUP BY 1,2,3,5,6,7,8,9
) AS a
ORDER BY lower(a."Branch");

#
# Find Items that are attached to deleted bibs
#
items_attached_to_deleted_bibs~~select 
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
aou.name AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item attached to deleted bib$$ AS "Issue",
ac.id AS "copyid"
from 
biblio.record_entry bre,
asset.copy ac
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!),
actor.org_unit aou,
asset.call_number acn,
asset.copy_location acl
where
sil.report IS NULL AND
aou.id=ac.circ_lib AND
bre.id=acn.record AND
acn.id=ac.call_number AND
acl.id=ac.location AND
bre.deleted AND
bre.id > 0 and bre.id < 10000 AND
NOT ac.deleted;

#
# Find Items that are attached to Electronic bibs
#
electronic_book_with_physical_items_attached~~select 
bre.id AS "Bib ID",
ac.barcode AS "Barcode",
acn.label AS "Call Number",
(SELECT string_agg(value,$$ $$) "FORMAT" FROM metabib.record_attr_flat WHERE attr=$$icon_format$$ AND id=bre.id GROUP BY id) AS "OPAC Icon",
aou.name AS "Branch",
(SELECT name FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "System",
(SELECT id FROM actor.org_unit_ancestor_at_depth(aou.id,1)) AS "SystemID",
$$item attached to electronic bib$$ AS "Issue",
ac.id AS "copyid"
from 
biblio.record_entry bre,
asset.copy ac
LEFT JOIN seekdestroy.ignore_list sil ON (sil.target_copy=ac.id and report=!!!reportid!!!),
actor.org_unit aou,
asset.call_number acn,
asset.copy_location acl
where
sil.report IS NULL AND
aou.id=ac.circ_lib AND
bre.id=acn.record AND
acn.id=ac.call_number AND
acl.id=ac.location AND
bre.id > 0 and bre.id < 1000000 AND
NOT ac.deleted AND
lower(bre.marc) ~ $$<datafield tag="856" ind1="4" ind2="0">$$ AND
(
    BRE.marc ~ $$tag="008">.......................[oqs]$$
    OR
    BRE.marc ~ $$tag="006">......[oqs]$$
)
AND
(
    BRE.marc ~ $$<leader>......[at]$$
)
AND
(
    BRE.marc ~ $$<leader>.......[acdm]$$
);