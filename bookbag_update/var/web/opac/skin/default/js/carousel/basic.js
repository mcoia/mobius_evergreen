
jQuery.noConflict();
mycarousel_itemList = {};
timervar = 0;
donecars = {};
template = {};
bb_total = {};
//mycarousel_itemList.sort(function() { return 0.5 - Math.random();});
function compileList(cid)
{	
	
	mycarousel_itemList[cid] = [];
	jQuery(".bbitem_"+cid).each(function(){
		var titlee="";
		var bibidd="";
		jQuery(this).children("a").each(function(){
			bibidd=jQuery(this).attr("href");
			titlee=jQuery(this).html();
		});
		mycarousel_itemList[cid].push({bibid: ""+bibidd, title: ""+titlee});
	});
}

function countCompleteAnchors(cid)
{	
	var ret=0;
	//alert("counting "+cid);
	jQuery(".bbitem_"+cid).each(function(){
		jQuery(this).children("a").each(function(){		
			if(jQuery(this).attr("href") != '#')
			{
				ret++;
			}
		});
	});
	//alert("Returning "+cid+" - "+ret);
	return ret;
}

// function clearTheInterval(cid)
// {
	// clearInterval(timervar[cid]);
// }

function checkAllCarousels(){
	if(donecars.length==bbags.length) 
	{
		clearInterval(timervar);
		return;
	}
	
	for(var i=0;i<bbags.length;i++){
		if(!donecars[bbags[i]])
		{
			var cid = bbags[i];
			
			if(countCompleteAnchors(cid) > (bb_total[cid]/2))
			{
				compileList(cid); 
			   jQuery('#mycarousel_'+cid).jcarousel({
					size: bb_total[cid],
					visible: 8,
					itemLoadCallback: {onBeforeAnimation:
										function(carousel, state)
										{
											
											if(countCompleteAnchors(cid) != bb_total[cid])
												compileList(cid); 
											for (var t = carousel.first; t <= carousel.last; t++) {
												if (carousel.has(t)) {
													continue;
												}

												if (t > bb_total[cid]) {
													break;
												}
												
												carousel.add(t,mycarousel_getItemHTML(mycarousel_itemList[cid][t-1]));
											}
											
										}
					} 
				});
				donecars[cid]=1;
				break;
			}
		}
	}
}
/**
 * Item html creation helper.
 */
function mycarousel_getItemHTML(item)
{
	var url = document.URL;
	var res = url.split("/"); 
	var total = ''+res[0]+'/'+res[1]+'/'+res[2]+'/';	
    return '<a target="_top" href="'+total+'eg/opac/record/' + item.bibid + '"><img src="'+total+'opac/extras/ac/jacket/medium/r/' + item.bibid + '" width="96" onerror="this.src=\'/opac/images/blank.png\'" height="150" alt="' + item.title + '" /><br />' + item.title + '</a>';
};



jQuery(document).ready(function() {	

		timervar = setInterval(checkAllCarousels, 1000);
});


