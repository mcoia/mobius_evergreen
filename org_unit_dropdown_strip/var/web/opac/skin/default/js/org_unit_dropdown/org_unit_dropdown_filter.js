var timer;
jQuery(document).ready(function() 
{	
	//alert(document.cookie);
	var currentScope = findLevel(jQuery("#search_org_selector").find('option:selected').html());
	var cook = getCookie('defaultLoc');
	
	//Dont do anything if we are scoped to the consortium or don't have the cookie set
	 if( (currentScope != 'consortium') || (cook > 0) )
	 {
		readOriginalDropdown();
		
		jQuery('#search_locg_label').append("<div id='showmoredropdowndiv' class ='toggleorgtreedropdown'><a href='#' id='showmoredropdown'>Show more locations</a></div>");
		jQuery('#search_locg_label').append("<div id='showlessdropdowndiv' class ='toggleorgtreedropdown'><a href='#' id='showlessdropdown'>Show fewer locations</a></div>");
		jQuery('#showmoredropdown').click(function(){
			refillDropdown(1);		
		});
		
		jQuery('#showlessdropdown').click(function(){
			stripDropDown(1);		
		});
		
		var t = jQuery("#search_org_selector").find('option:selected').attr('value');
		//Only strip the dropdown if we are currently at our defaultScope
		if(getCookie('defaultLoc') == t)
		{
			stripDropDown(0);
		}
		else
		{
			jQuery('#showmoredropdowndiv').hide();
			jQuery('#showlessdropdowndiv').show();
		}
	}
});

function readOriginalDropdown()
{
	var og = jQuery("#search_org_selector");
	jQuery('body').append("<select id='originalorgunitdropdown'>"+og.html()+"</select>");
	jQuery('#originalorgunitdropdown').hide();
	var t = jQuery("#search_org_selector").find('option:selected').attr('value');
	//jQuery('#homesearch_main_logo').append("<br /><br /><br />"+t);
	var test = getCookie('defaultLoc')
	if(test.length == 0)
	{	
		//jQuery('#homesearch_main_logo').append("Setting cookie<br />");
		setCookie('defaultLoc',t,.2);
	}
}

function stripDropDown(visualFeedback)
{
	//jQuery('#homesearch_main_logo').append("<br /><br /><br />");
	var cookie = getCookie('defaultLoc');
	var selectedOU = getOrgHTMLFromID(cookie);
	//jQuery('#homesearch_main_logo').append("<br /><br /><br />"+selectedOU);
	//jQuery("#search_org_selector").find('option:selected').html();
	if(
	(findLevel(selectedOU) == 'branch') ||
	(findLevel(selectedOU) == 'bookmobile')	||
	(findLevel(selectedOU) == 'system')
	)
	{
		var group = findSiblings(selectedOU);		
		jQuery("#search_org_selector").children().each(function()
		{
			var thisone = jQuery(this).html();
			if(group.indexOf(thisone) == -1)
			{
				jQuery(this).remove();
			}
		});
	}
	jQuery("#search_org_selector").find('option:selected').removeAttr('selected');
	jQuery("#search_org_selector").find('option[value="'+cookie+'"]').attr('selected',true);
	jQuery('#showmoredropdowndiv').show();
	jQuery('#showlessdropdowndiv').hide();
	if(visualFeedback==1)
	{
		jQuery("#search_org_selector").css("background-color","hsl(120,60%,70%)");
		timer=setInterval(clearGreenFeedback, 1500);
	}
}

function refillDropdown(visualFeedback)
{
	var og = jQuery("#originalorgunitdropdown");
	jQuery("#search_org_selector").html(og.html());
	jQuery('#showmoredropdowndiv').hide();
	jQuery('#showlessdropdowndiv').show();
	if(visualFeedback==1)
	{
		jQuery("#search_org_selector").css("background-color","hsl(120,60%,70%)");
		timer=setInterval(clearGreenFeedback, 1500);
	}
}

function findSiblings(branchText)
{
	var ret = [];
	var thisgroup = new Array();
	var foundmatch=0;
	var stoprecording = 0;
	var consort = '';
	jQuery("#search_org_selector").children().each(function(){
		var thislevel = findLevel(jQuery(this).html());
		if(stoprecording==0)
		{
			if(thislevel == 'system')
			{
				if(foundmatch==0)
				{
					thisgroup =  new Array();
					thisgroup.push(consort);
					thisgroup.push(jQuery(this).html());
				}
				else
				{	
					stoprecording = 1;
				}
			}
			else if(thislevel == 'branch')
			{	
				thisgroup.push(jQuery(this).html());
			}
			else if(thislevel == 'bookmobile')
			{
				thisgroup.push(jQuery(this).html());
			}
			else if(thislevel == 'consortium')
			{
				consort = jQuery(this).html();
				thisgroup.push(consort);
			}
			if(jQuery(this).html() == branchText)
			{
				foundmatch=1;
			}
		}
	});
	return thisgroup;
}

function getOrgHTMLFromID(theid)
{
	var ret='';
	jQuery("#originalorgunitdropdown").children().each(function(){
		var thisid = jQuery(this).attr('value');
		if(thisid==theid)
		{
			ret = jQuery(this).html();
		}
	});
	return ret;
}

function findLevel(branchText)
{
	var num = branchText.split("&nbsp;");
	switch(num.length)
	{
		case 2:
			return 'consortium';
		case 4:
			return 'system';
		case 6:
			return 'branch';
		case 8:
			return 'bookmobile';
		default: 
			return 'unknown';
	}
}

function getCookie(cname) {
    var name = cname + "=";
    var ca = document.cookie.split(';');
    for(var i=0; i<ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0)==' ') c = c.substring(1);
        if (c.indexOf(name) == 0) return c.substring(name.length,c.length).replace(/v0v/g,';').replace(/v1v/g,'&');
    }
    return "";
}

function setCookie(cname, cvalue, exdays) {
	cvalue = cvalue.replace(/;/g,'v0v').replace(/&/g,'v1v');
    var d = new Date();
    d.setTime(d.getTime() + (exdays*24*60*60*1000));
    var expires = "expires="+d.toUTCString();
	//jQuery('#homesearch_main_logo').append("Setting: "+cvalue+"<br />");
	var finalc = cname + "="
	+ cvalue
	+ "; " + expires
	+ "; path=/";
	//+ cvalue
    document.cookie = finalc;
	
}

function clearGreenFeedback()
{
	 //jQuery("#search_org_selector").css("background-color","white");
	 clearInterval(timer);
	 var d = 1000;
	for(var i=50; i<=100; i=i+0.5){ //i represents the lightness
		d  += 10;
		(function(ii,dd){
			setTimeout(function(){
				jQuery("#search_org_selector").css('background-color','hsl(120,60%,'+ii+'%)'); 
			}, dd);
		})(i,d);
	}
}
