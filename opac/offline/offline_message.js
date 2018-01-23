jQuery(document).ready(function() {	
	// var msg = 'This website and other ILS services will be unavailable September 3 at 7pm';
	var msg = '';
	if(msg.length > 0)
	{
		var dismissed = getCookie('dismiss_offline_msg');
		if(dismissed.length == 0)
		{
			jQuery("body").prepend("<div class='offline_msg'><p>"+msg+"</p></div>");
			jQuery(".offline_msg p").append("<span class='dismiss_offline_msg'>DISMISS</span>");
			jQuery(".dismiss_offline_msg").click( function(data) {
				setCookie('dismiss_offline_msg','t','.2');
				jQuery(".offline_msg").remove();
			});
		}
	}

});
