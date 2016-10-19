/* XXX allow to pass in a 'local' var so the links back into the opac can be localized */
/* maybe also a 'skin' var */

function bbInit(bb) {
	if(!bb) { return; }
	new OpenSRF.ClientSession('open-ils.actor').request({
		method: 'open-ils.actor.container.public.flesh',
		params: ['biblio', bb],
		async: true,
		oncomplete: bbShow
	}).send();
}

function bbShow(r) {

	var resp = r.recv();
	if (!resp) { return; }
	var bb = resp.content();
	if(!bb || !bb.pub()) { return; }
	var thisid = bb.id();	
	bb_total[thisid]=bb.items().length;
	
	$('bb_name_'+thisid).appendChild(text(bb.name()));

	var tbody = $('bbitems_'+thisid);
	
	if(!template[thisid]) 	
		template[thisid] = tbody.removeChild($('row_template_'+thisid));
	
	for( var i in bb.items() ) 
		tbody.appendChild(bbShowItem( template[thisid], bb.items()[i] ));
}

function bbShowItem( template, item ) {
	var row = template.cloneNode(true);
	var tlink = $n(row, 'title');
	var alink = $n(row, 'author');	

	new OpenSRF.ClientSession('open-ils.search').request({
		method: 'open-ils.search.biblio.record.mods_slim.retrieve',
		params: [item.target_biblio_record_entry()],
		aysnc: true,
		oncomplete: function(r) {
			var resp = r.recv();
			if (!resp) { return; }
			var rec = resp.content();
			buildTitleDetailLink(rec, tlink); 
			tlink.setAttribute('href', ''+rec.doc_id());
			alink.appendChild(text(rec.author()));
		}
	}).send();
		
	return row;
}

jQuery(document).ready(function(){
	for(var i=0;i<bbags.length;i++){
	bb_total[bbags[i]] = 10000000;
		jQuery('#hidden_bb_info').append(
		"<div id='bbitems_"+bbags[i]+"'><div id='row_template_"+bbags[i]+"' class='bbitem_"+bbags[i]+"'><a href='#' name='title' class='bbtitle_"+bbags[i]+"'> </a><span name='author'> </span></div></div>");
		jQuery('#carousels').append(
		"<div><div id='bb_name_"+bbags[i]+"' class='carousel_title'> </div><div class='wrap'>  <ul id='mycarousel_"+bbags[i]+"' class='jcarousel-skin-meskin'>  </ul></div></div>");
		
		
		bbInit(bbags[i]);
	}
});
