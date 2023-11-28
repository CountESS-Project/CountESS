// from https://stackoverflow.com/a/67552284
// by CMSG https://stackoverflow.com/users/8469099/cmsg
// slightly modified by Nick to keep heading id if id already exists

// Let the DOM content load before running the script.
window.addEventListener('DOMContentLoaded', function (event) {
    //Get all headings only from the actual contents.
    var contentContainer = document.getElementById('content'); // Add this div to the html

    var headings = contentContainer.querySelectorAll('h1,h2,h3,h4,h5'); // You can do as many or as few headings as you need.

    // NICK: Don't bother running if no or only one heading

    if (headings.length < 2) return;

    var tocContainer = document.getElementById('toc'); // Add this div to the HTML
    // create ul element and set the attributes.
    var ul = document.createElement('ul');

    ul.setAttribute('id', 'tocList');
    ul.setAttribute('class', 'sidenav')

    // Loop through the headings NodeList
    for (i = 0; i <= headings.length - 1; i++) {

	// NICK: keep id if there already is one.

        var id = headings[i].id || headings[i].innerHTML.toLowerCase().replace(/ /g, "-"); // Set the ID to the header text, all lower case with hyphens instead of spaces.
        var level = headings[i].localName.replace("h", ""); // Getting the header a level for hierarchy
        var title = headings[i].innerHTML; // Set the title to the text of the header
    
        headings[i].setAttribute("id", id)  // Set header ID to its text in lower case text with hyphens instead of spaces.

        var li = document.createElement('li');     // create li element.
        li.setAttribute('class', 'sidenav__item') // Assign a class to the li
        var a = document.createElement('a'); // Create a link
        a.setAttribute("href", "#" + id) // Set the href to the heading ID
        a.innerHTML = title; // Set the link text to the heading text
        
        // Create the hierarchy

        if(level == 1) {
            li.appendChild(a); // Append the link to the list item
            ul.appendChild(li);     // append li to ul.
        } else if (level == 2) {
            child = document.createElement('ul'); // Create a sub-list
            child.setAttribute('class', 'sidenav__sublist')
            li.appendChild(a); 
            child.appendChild(li);
            ul.appendChild(child);
        } else if (level == 3) {
            grandchild = document.createElement('ul');
            grandchild.setAttribute('class', 'sidenav__sublist')
            li.appendChild(a);
            grandchild.appendChild(li);
            child.appendChild(grandchild);
        } else if (level == 4) {
            great_grandchild = document.createElement('ul');
            great_grandchild.setAttribute('class', 'sidenav__sublist');
            li.append(a);
            great_grandchild.appendChild(li);
            grandchild.appendChild(great_grandchild);
        } else if (level == 5) {
            great_great_grandchild = document.createElement('ul');
            great_great_grandchild.setAttribute('class', 'sidenav__sublist');
            li.append(a);
            great_great_grandchild.appendChild(li);
            great_grandchild.appendChild(great_great_grandchild);
        }
    }

    tocContainer.appendChild(ul);       // add list to the container
    
    // Add a class to the first list item to allow for toggling active state.
    var links = tocContainer.getElementsByClassName("sidenav__item");
    
    links[0].classList.add('current');
    
    // Loop through the links and add the active class to the current/clicked link
    for (var i = 0; i < links.length; i++) {
        links[i].addEventListener("click", function() {
            var current = document.getElementsByClassName("current");
            current[0].className = current[0].className.replace(" current", "");
            this.className += " current";
        });
    }
});

