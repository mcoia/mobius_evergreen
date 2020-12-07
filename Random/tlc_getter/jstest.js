
var readContents = [];
var readContentsDone = false;

function changePage()
{
    var doms = document.getElementsByTagName('a');
    var stop = 0;
    for(var i=0;i<doms.length;i++)
    {
        if(!stop)
        {
            var thisaction = doms[i].getAttribute('onClick');

            if(thisaction !=undefined && thisaction.match(/\('nextPage'\)/g))
            {
                doms[i].click();
                stop = 1;
            }
        }
    }
    if(!stop)
    {
        readContentsDone = true;
        return false;
    }
    else
    {
       pageChanged();
    }
    return 1;
}

function pageChanged()
{
    var doms = document.getElementsByClassName('modalWaitPage');
    if(doms.length > 0)
    {
        setTimeout(pageChanged,10);
    }
    else
    {
        readThisPage(false);
        changePage();
    }
}

function downloadCSV()
{
    
    var tab = document.getElementById('_NS_runIn');
    tab.dispatchEvent(new MouseEvent('mouseup', { 'bubbles': true }));

    tab = document.getElementById('_NS_viewInExcel');
    tab.dispatchEvent(new MouseEvent('mouseover', { 'bubbles': true }));

    tab = document.getElementById('_NS_viewInCSV');
    tab.dispatchEvent(new MouseEvent('mouseup', { 'bubbles': true }));

    return 1;
}

function readThisPage(includeHeader = false)
{
    var doms = document.getElementsByTagName('table');
    var stop = 0;
    var contentTableDom;
    for(var i=0;i<doms.length;i++)
    {
        if(!stop)
        {
            var thisaction = doms[i].getAttribute('class');

            if(thisaction!=undefined && thisaction.match(/ls/g))
            {
                stop = 1;
                contentTableDom = doms[i];
            }
        }
    }
    if(!stop)
    {
        // return 'No output table';
    }
    else
    {
        var firstRow = true;
        for (var i = 0, row; row = contentTableDom.rows[i]; i++)
        {
            var thisRow = [];
            var addedData = false;
            for (var j = 0, col; col = row.cells[j]; j++)
            {
                if(firstRow && includeHeader)
                {
                    thisRow.push(col.innerText);
                    addedData = true;
                }
                else if (!firstRow)
                {
                    thisRow.push(col.innerText);
                    addedData = true;
                }
            }
            if(addedData)
            {
                readContents.push(thisRow);
            }
            
            firstRow = false;
        }
    }
    //  return 1;
}

// readThisPage(true);

// changePage();

// console.log(changed);

console.log(downloadCSV());
//console.log(readContents);