
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

function showMore520(jdom)
{
    var phrase = jdom.html();
    if( phrase == 'Show More') {jdom.html('Show Less')}
    else {jdom.html('Show More')}
    jdom.parent().children('.hiding520').toggle();
}

jQuery(document).ready(function() {
    jQuery(".search_result_520_content").each(function(index){
        var content = jQuery(this).html();
        if(content.length > 50)
        {
            var showing = content.substring(1,150);
            var hiding = content.substring(150);
            var span = showing+"<span><span style='display:none' class='hiding520'>"+hiding+"</span><a href='#' class='showmore520'>Show More</a></span>";
            jQuery(this).html(span);
        }
    });
    jQuery(".showmore520").click(function(){showMore520(jQuery(this)); return false;});
});
