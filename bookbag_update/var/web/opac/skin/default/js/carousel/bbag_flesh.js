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
	
	//Try something new
	//console.log("Process bookbag items");
	//console.log(bb);
	//Create array of book bag item bre.id
	var bbids = bb.items().map(function (item) {
		return item.target_biblio_record_entry()
	});
	//console.log(bbids);

	//Obj with bibid key and value order for use later
	var bbitemsorder = {};
	for ( var i in bb.items() ) {
		//use pos if available, otherwise use bucket item id for order
		bbitemsorder[bb.items()[i].target_biblio_record_entry()]=
		( bb.items()[i].pos() ? bb.items()[i].pos() : bb.items()[i].id() );
	}
	//console.log(bbitemsorder);
        
	//Flesh all titles at once
	new OpenSRF.ClientSession('open-ils.search').request({
		method: 'open-ils.search.biblio.record.mods_slim.retrieve',
		params: [bbids],
		aysnc: true,
		oncomplete: function(r) {
			var resp = r.recv();
			if (!resp) { return; }
			var items = resp.content();
			//console.log(items);

			//add sort position to data structure
			// so we can use it later
			for (var i in items ) {
				items[i].pos=bbitemsorder[items[i].doc_id()];
			}
			//console.log(items);

			//Use the sort position to feed items to carousel in order
			for( var i in items.sort(function(a, b){return a.pos - b.pos}) ) {
				tbody.appendChild(bbShowItem( template[thisid], items[i] ));
			}
		}
	}).send();
}

function bbShowItem( template, item ) {

	//console.log("Deal with the items");
	//console.log(item);

	var row = template.cloneNode(true);
	var tlink = $n(row, 'title');
	var alink = $n(row, 'author');

	buildTitleDetailLink(item, tlink);
	tlink.setAttribute('href', ''+item.doc_id());
	alink.appendChild(text(item.author()));

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
