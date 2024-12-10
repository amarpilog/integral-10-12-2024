/*
 * Dependencies:
 * - d3.js
 * - jquery.js
 */
"use strict";

function treeBoxes(urlService, jsonData) {
    var urlService_ = '';
    var treeInitialized = jsonData['init'];
    var minHeight = jsonData['minHeight'];
    var minWidth = jsonData['minWidth'];
    var nodeColor = jsonData['color'];
//    var depthSelector = parseInt($("#depthSelector").val());
    var depthSelector = jsonData['depthLevel'];
    var blue = '', //#337ab7',
            green = '', //'#5cb85c',
            yellow = '', //'#f0ad4e',
            blueText = '', //'#4ab1eb',
            purple = '';//'#9467bd';
    var hideChild = jsonData['collapse'];//21422

    var margin = {
        top: 0,
        right: 0,
        bottom: 200,
        left: 0
    },
            // Height and width are redefined later in function of the size of the tree
            // (after that the data are loaded)
            width = 1000 - margin.right - margin.left,
            height = 800 - margin.top - margin.bottom;

    var rectNode = {width: 120, height: 45, textMargin: 5},
            tooltip = {width: 150, height: 40, textMargin: 5};
    var i = 0,
            duration = 750,
            root;

    var mousedown; // Use to save temporarily 'mousedown.zoom' value
    var mouseWheel,
            mouseWheelName,
            isKeydownZoom = false;

    var tree;
    var baseSvg,
            svgGroup,
            nodeGroup, // If nodes are not grouped together, after a click the svg node will be set after his corresponding tooltip and will hide it
            nodeGroupTooltip,
            linkGroup,
            linkGroupToolTip,
            defs;

    init(urlService, jsonData);

    function collapse(d) {
        if (d.children) {
            d._children = d.children;
            d._children.forEach(collapse);
            d.children = null;
        }
    }
    function collapsePrevNode(root) {           //19422
        if (root['prevNode'] != undefined && root['prevNode'].children) {
            if (root['prevNode']) {
                root['prevNode']._children = root['prevNode'].children;
                root['prevNode']._children.forEach(collapsePrevNode);
                if (root['prevNode']._children) {
                    root['prevNode'].children = null;
                }
            }
        }
    }
    function collapsePrevParentNode(root) {           //19422
        if (root['parentPrevNode'] != undefined && root['parentPrevNode'].children) {
            if (root['parentPrevNode']) {
                root['parentPrevNode']._children = root['parentPrevNode'].children;
//                root['parentPrevNode']._children.forEach(collapsePrevNode);
                root['parentPrevNode'].children.forEach(function (d) {//29422
                    if (d.children) {
                        d._children = d.children;
                        d.children = null;
                    }
                });//29422
                root['parentPrevNode'].children = null;

                if (root['parentPrevNode']._children) {
                    root['parentPrevNode'] = null;
                }
            }
        }
    }

    function init(urlService, jsonData)
    {
        urlService_ = urlService;
        if (urlService && urlService.length > 0)
        {
            if (urlService.charAt(urlService.length - 1) != '/')
                urlService_ += '/';
        }

        if (jsonData)
            drawTree(jsonData);
        else
        {
            console.error(jsonData);
            alert('Invalides data.');
        }
    }

    function drawTree(jsonData)
    {
        tree = d3.layout.tree().size([height, width]);
        root = jsonData;
        root.fixed = true;

        // Dynamically set the height of the main svg container
        // breadthFirstTraversal returns the max number of node on a same level
        // and colors the nodes
        var maxDepth = 0;
        var maxTreeWidth = breadthFirstTraversal(tree.nodes(root), function (currentLevel) {
            maxDepth++;
            if (!hideChild || hideChild == undefined || hideChild == null)//27422
            {
                if (nodeColor != null && nodeColor != '' && nodeColor != undefined) {
                    currentLevel.forEach(function (node) {
                        if (depthSelector == undefined || depthSelector == null)
                            node.color = '#AE7A9B';
                    });
                }
            }
        });

        if (minHeight == undefined || minHeight == null) {
            height = maxTreeWidth * (rectNode.height + (-40)) + tooltip.height + (-40) - margin.right - margin.left;
            width = maxDepth * (rectNode.width * 2) + tooltip.width / 1 - margin.top - margin.bottom;
        }

        if (treeInitialized) {
            deepCollapse(jsonData);
        } else {
            jsonData['minHeight'] = height;
            jsonData['minWidth'] = width;
        }
        tree = d3.layout.tree().size([height, width]);
        root.x0 = height / 2;
        root.y0 = 0;

        baseSvg = d3.select('#tree-container').append('svg')
                .attr('width', width + margin.right + margin.left)
                .attr('height', height + margin.top + margin.bottom)
                .attr('class', 'svgContainer')
                .call(d3.behavior.zoom()
                        //.scaleExtent([0.5, 1.5]) // Limit the zoom scale
                        .on('zoom', zoomAndDrag));

        // Mouse wheel is desactivated, else after a first drag of the tree, wheel event drags the tree (instead of scrolling the window)
        getMouseWheelEvent();
        d3.select('#tree-container').select('svg').on(mouseWheelName, null);
        d3.select('#tree-container').select('svg').on('dblclick.zoom', null);

        svgGroup = baseSvg.append('g')
                .attr('class', 'drawarea')
                .append('g')
                .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
        nodeGroup = svgGroup.append('g')
                .attr('id', 'nodes');
        linkGroup = svgGroup.append('g')
                .attr('id', 'links');
        linkGroupToolTip = svgGroup.append('g')
                .attr('id', 'linksTooltips');
        nodeGroupTooltip = svgGroup.append('g')
                .attr('id', 'nodesTooltips');

        defs = baseSvg.append('defs');
        initArrowDef();
        initDropShadow();

        update(root);
    }

    function update(source) {
        var nodes = tree.nodes(root).reverse(),
                links = tree.links(nodes);

        // Check if two nodes are in collision on the ordinates axe and move them
        breadthFirstTraversal(tree.nodes(root), collision);
        // Normalize for fixed-depth
        nodes.forEach(function (d) {
            d.y = d.depth * (rectNode.width * 1.5);
        });

        // 1) ******************* Update the nodes *******************
        var node = nodeGroup.selectAll('g.node').data(nodes, function (d) {
            return d.id || (d.id = ++i);
        });
        var nodesTooltip = nodeGroupTooltip.selectAll('g').data(nodes, function (d) {
            return d.id || (d.id = ++i);
        });
        var nodeEnter = node.enter().insert('g', 'g.node')
                .attr('class', 'node')
                .attr('transform', function (d) {
                    return 'translate(' + source.y0 + ',' + source.x0 + ')';
                })
                .on('click', function (d) {
                    click(d);
                });
        var nodeEnterTooltip = nodesTooltip.enter().append('g')
                .attr('transform', function (d) {
                    return 'translate(' + source.y0 + ',' + source.x0 + ')';
                });

        nodeEnter.append('g').append('rect')
                .attr('rx', 6)
                .attr('ry', 6)
                .attr('width', rectNode.width)
                .attr('height', rectNode.height)
                .attr('class', 'node-rect')
                .attr('nodeDepth', function (d) {

                    if (d["depth"] == 0)
                        return 'nodeDepthLevel0';
                    if (d["depth"] == 1)
                        return 'nodeDepthLevel1';
                    if (d["depth"] == 2)
                        return 'nodeDepthLevel2';
                    if (d["depth"] == 3)
                        return 'nodeDepthLevel3';
                    if (d["depth"] == 4)
                        return 'nodeDepthLevel4';
                })
                .attr('fill', function (d) {
                    if (d["depth"] == 0)
                        return $("#colorAdderlevel0").val();
                    if (d["depth"] == 1)
                        return $("#colorAdderlevel1").val();
                    if (d["depth"] == 2)
                        return $("#colorAdderlevel2").val();
                    if (d["depth"] == 3)
                        return $("#colorAdderlevel3").val();
                    if (d["depth"] == 4)
                        return $("#colorAdderlevel4").val();
                })
                .attr('filter', 'url(#drop-shadow)');

        nodeEnter.append('foreignObject')
                .attr('x', rectNode.textMargin)
                .attr('y', rectNode.textMargin)
                .attr('width', function () {
                    return (rectNode.width - rectNode.textMargin * 2) < 0 ? 0
                            : (rectNode.width - rectNode.textMargin * 2)
                })
                .attr('height', function () {
                    return (rectNode.height - rectNode.textMargin * 2) < 0 ? 0
                            : (rectNode.height - rectNode.textMargin * 2)
                })
                .append('xhtml').html(function (d) {
            return '<div style="width: '
                    + (rectNode.width - rectNode.textMargin * 2) + 'px; height: '
                    + (rectNode.height - rectNode.textMargin * 2) + 'px;" class="node-text wordwrap">'
                    + '<b>' + d.nodeName + '</b><br><br>'
//                    + '<b>Plant: </b>' + d.plant + '<br>'
//                    + '<b>Instance: </b>' + d.instance + '<br>'
                    + '</div>';
        })
                .on('mouseover', function (d) {
                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
                })
                .on('mouseout', function (d) {
                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
                });

        nodeEnterTooltip.append("rect")
                .attr('id', function (d) {
                    return 'nodeInfoID' + d.id;
                })
                .attr('x', rectNode.width / 2)
                .attr('y', rectNode.height / 2)
                .attr('width', tooltip.width)
                .attr('height', tooltip.height)
                .attr('class', 'tooltip-box')
                .style('fill-opacity', 0.8)
                .on('mouseover', function (d) {
                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
                    removeMouseEvents();
                })
                .on('mouseout', function (d) {
                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
                    reactivateMouseEvents();
                });

        nodeEnterTooltip.append("text")
                .attr('id', function (d) {
                    return 'nodeInfoTextID' + d.id;
                })
                .attr('x', rectNode.width / 2 + tooltip.textMargin)
                .attr('y', rectNode.height / 2 + tooltip.textMargin * 2)
                .attr('width', tooltip.width)
                .attr('height', tooltip.height)
                .attr('class', 'tooltip-text')
                .style('fill', 'white')
                .append("tspan")
                .text(function (d) {
                    return 'Name: ' + d.name;
                })
                .append("tspan")
                .attr('x', rectNode.width / 2 + tooltip.textMargin)
                .attr('dy', '1.5em')
                .text(function (d) {
                    return '';
                })
                .append("tspan")
                .attr('x', rectNode.width / 2 + tooltip.textMargin)
                .attr('dy', '1.5em')
                .text(function (d) {
                    return 'Demand: 18';
                });

        // Transition nodes to their new position.
        var nodeUpdate = node.transition().duration(duration)
                .attr('transform', function (d) {
                    return 'translate(' + d.y + ',' + d.x + ')';
                });
        nodesTooltip.transition().duration(duration)
                .attr('transform', function (d) {
                    return 'translate(' + d.y + ',' + d.x + ')';
                });

        nodeUpdate.select('rect')
                .attr('class', function (d) {
                    return d._children ? 'node-rect-closed' : 'node-rect';
                });

        nodeUpdate.select('text').style('fill-opacity', 1);

        // Transition exiting nodes to the parent's new position
        var nodeExit = node.exit().transition().duration(duration)
                .attr('transform', function (d) {
                    return 'translate(' + source.y + ',' + source.x + ')';
                })
                .remove();
        nodesTooltip.exit().transition().duration(duration)
                .attr('transform', function (d) {
                    return 'translate(' + source.y + ',' + source.x + ')';
                })
                .remove();

        nodeExit.select('text').style('fill-opacity', 1e-6);


        // 2) ******************* Update the links *******************
        var link = linkGroup.selectAll('path').data(links, function (d) {
            return d.target.id;
        });
        var linkTooltip = linkGroupToolTip.selectAll('g').data(links, function (d) {
            return d.target.id;
        });

        function linkMarkerStart(direction, isSelected) {
            if (direction == 'SYNC')
            {
                return isSelected ? 'url(#start-arrow-selected)' : 'url(#start-arrow)';
            }
            return '';
        }

        function linkType(link) {
            if (link.direction == 'SYNC')
                return "Synchronous [\u2194]";
            else
            {
                if (link.direction == 'ASYN')
                    return "Asynchronous [\u2192]";
            }
            return '???';
        }

        d3.selection.prototype.moveToFront = function () {
            return this.each(function () {
                this.parentNode.appendChild(this);
            });
        };

        // Enter any new links at the parent's previous position.
        // Enter any new links at the parent's previous position.
        var linkenter = link.enter().insert('path', 'g')
                .attr('class', 'link')
                .attr('id', function (d) {
                    return 'linkID' + d.target.id;
                })
                .attr('d', function (d) {
                    return diagonal(d);
                })
                .attr('marker-end', 'url(#end-arrow)')
                .attr('marker-start', function (d) {
                    return linkMarkerStart(d.target.link.direction, false);
                })
                .on('mouseover', function (d) {
                    d3.select(this).moveToFront();

                    d3.select(this).attr('marker-end', 'url(#end-arrow-selected)');
                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, true));
                    d3.select(this).attr('class', 'linkselected');

                    $('#tooltipLinkID' + d.target.id).attr('x', (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y);
                    $('#tooltipLinkID' + d.target.id).attr('y', (d.target.x - d.source.x) / 2 + d.source.x);
                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
                })
                .on('mouseout', function (d) {
                    d3.select(this).attr('marker-end', 'url(#end-arrow)');
                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, false));
                    d3.select(this).attr('class', 'link');
                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
                });

        linkTooltip.enter().append('rect')
                .attr('id', function (d) {
                    return 'tooltipLinkID' + d.target.id;
                })
                .attr('class', 'tooltip-box')
                .style('fill-opacity', 0.8)
                .attr('x', function (d) {
                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y;
                })
                .attr('y', function (d) {
                    return (d.target.x - d.source.x) / 2 + d.source.x;
                })
                .attr('width', tooltip.width)
                .attr('height', tooltip.height)
                .on('mouseover', function (d) {
                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
                    // After selected a link, the cursor can be hover the tooltip, that's why we still need to highlight the link and the arrow
                    $('#linkID' + d.target.id).attr('class', 'linkselected');
                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow-selected)');
                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, true));

                    removeMouseEvents();
                })
                .on('mouseout', function (d) {
                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
                    $('#linkID' + d.target.id).attr('class', 'link');
                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow)');
                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, false));

                    reactivateMouseEvents();
                });

        linkTooltip.enter().append('text')
                .attr('id', function (d) {
                    return 'tooltipLinkTextID' + d.target.id;
                })
                .attr('class', 'tooltip-text')
                .attr('x', function (d) {
                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
                })
                .attr('y', function (d) {
                    return (d.target.x - d.source.x) / 2 + d.source.x + tooltip.textMargin * 2;
                })
                .attr('width', tooltip.width)
                .attr('height', tooltip.height)
                .style('fill', 'white')
                .append("tspan")
                .text(function (d) {
                    return linkType(d.target.link);
                })
                .append("tspan")
                .attr('x', function (d) {
                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
                })
                .attr('dy', '1.5em')
                .text(function (d) {
                    return d.target.link.name;
                });

        // Transition links to their new position.
        var linkUpdate = link.transition().duration(duration)
                .attr('d', function (d) {
                    return diagonal(d);
                });
        linkTooltip.transition().duration(duration)
                .attr('d', function (d) {
                    return diagonal(d);
                });

        // Transition exiting nodes to the parent's new position.
        link.exit().transition()
                .remove();

        linkTooltip.exit().transition()
                .remove();

        // Stash the old positions for transition.
        nodes.forEach(function (d) {
            d.x0 = d.x;
            d.y0 = d.y;
        });
    }

    // Zoom functionnality is desactivated (user can use browser Ctrl + mouse wheel shortcut)
    function zoomAndDrag() {
        //var scale = d3.event.scale,
        var scale = 1,
                translation = d3.event.translate,
                tbound = -height * scale,
                bbound = height * scale,
                lbound = (-width + margin.right) * scale,
                rbound = (width - margin.left) * scale;
        // limit translation to thresholds
        translation = [
            Math.max(Math.min(translation[0], rbound), lbound),
            Math.max(Math.min(translation[1], bbound), tbound)
        ];
        d3.select('.drawarea')
                .attr('transform', 'translate(' + translation + ')' +
                        ' scale(' + scale + ')');
    }

    // Toggle children on click.
    function click(d) {
        if (d['children'] || d['_children']) {
            if (d.children) {
                d._children = d.children;
                d.children = null;
            } else {
                d.children = d._children;
                d._children = null;
                var nodeDepth = d.depth;
                if (root['prevDepth' + nodeDepth] && root['prevDepth' + nodeDepth] != undefined && root['prevDepth' + nodeDepth] != null) {
                    if (d.id != root['prevDepth' + nodeDepth]['id'])
                        collapseDepth(root['prevDepth' + nodeDepth], nodeDepth);
                }
                root['prevDepth' + nodeDepth] = d;

            }
            update(d);
        }
    }
    //17522
    function collapseDepth(d, x) {
        if (d.children) {
            d._children = d.children;
            d.children.forEach(function (d) {
                if (d.children) {
                    d._children = d.children;
                    d.children = null;
                }
            });
            d.children = null;
        }
    }

    // Breadth-first traversal of the tree
    // func function is processed on every node of a same level
    // return the max level
    function breadthFirstTraversal(tree, func)
    {
        var max = 0;
        if (tree && tree.length > 0)
        {
            var currentDepth = tree[0].depth;
            var fifo = [];
            var currentLevel = [];

            fifo.push(tree[0]);
            while (fifo.length > 0) {
                var node = fifo.shift();
                if (node.depth > currentDepth) {
                    func(currentLevel);
                    currentDepth++;
                    max = Math.max(max, currentLevel.length);
                    currentLevel = [];
                }
                currentLevel.push(node);
                if (node.children) {
                    for (var j = 0; j < node.children.length; j++) {
                        fifo.push(node.children[j]);
                    }
                }
            }
            func(currentLevel);
            return Math.max(max, currentLevel.length);
        }
        return 0;
    }

    // x = ordoninates and y = abscissas
    function collision(siblings) {
        var minPadding = 5;
        if (siblings) {
            for (var i = 0; i < siblings.length - 1; i++)
            {
                if (siblings[i + 1].x - (siblings[i].x + rectNode.height) < minPadding)
                    siblings[i + 1].x = siblings[i].x + rectNode.height + minPadding;
            }
        }
    }

    function removeMouseEvents() {
        // Drag and zoom behaviors are temporarily disabled, so tooltip text can be selected
        mousedown = d3.select('#tree-container').select('svg').on('mousedown.zoom');
        d3.select('#tree-container').select('svg').on("mousedown.zoom", null);
    }

    function reactivateMouseEvents() {
        // Reactivate the drag and zoom behaviors
        d3.select('#tree-container').select('svg').on('mousedown.zoom', mousedown);
    }

    // Name of the event depends of the browser
    function getMouseWheelEvent() {
        if (d3.select('#tree-container').select('svg').on('wheel.zoom'))
        {
            mouseWheelName = 'wheel.zoom';
            return d3.select('#tree-container').select('svg').on('wheel.zoom');
        }
        if (d3.select('#tree-container').select('svg').on('mousewheel.zoom') != null)
        {
            mouseWheelName = 'mousewheel.zoom';
            return d3.select('#tree-container').select('svg').on('mousewheel.zoom');
        }
        if (d3.select('#tree-container').select('svg').on('DOMMouseScroll.zoom'))
        {
            mouseWheelName = 'DOMMouseScroll.zoom';
            return d3.select('#tree-container').select('svg').on('DOMMouseScroll.zoom');
        }
    }

    function diagonal(d) {
        var p0 = {
            x: d.source.x + rectNode.height / 2,
            y: (d.source.y + rectNode.width)
        }, p3 = {
            x: d.target.x + rectNode.height / 2,
            y: d.target.y - 12 // -12, so the end arrows are just before the rect node
        }, m = (p0.y + p3.y) / 2, p = [p0, {
                x: p0.x,
                y: m
            }, {
                x: p3.x,
                y: m
            }, p3];
        p = p.map(function (d) {
            return [d.y, d.x];
        });
        return 'M' + p[0] + 'C' + p[1] + ' ' + p[2] + ' ' + p[3];
    }

    function initDropShadow() {
        var filter = defs.append("filter")
                .attr("id", "drop-shadow")
                .attr("color-interpolation-filters", "sRGB");

        filter.append("feOffset")
                .attr("result", "offOut")
                .attr("in", "SourceGraphic")
                .attr("dx", 0)
                .attr("dy", 0);

        filter.append("feGaussianBlur")
                .attr("stdDeviation", 2);

        filter.append("feOffset")
                .attr("dx", 2)
                .attr("dy", 2)
                .attr("result", "shadow");

        filter.append("feComposite")
                .attr("in", 'offOut')
                .attr("in2", 'shadow')
                .attr("operator", "over");
    }

    function initArrowDef() {
        // Build the arrows definitions
        // End arrow
        defs.append('marker')
                .attr('id', 'end-arrow')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrow')
                .append('path')
                .attr('d', 'M0,-5L10,0L0,5');

        // End arrow selected
        defs.append('marker')
                .attr('id', 'end-arrow-selected')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrowselected')
                .append('path')
                .attr('d', 'M0,-5L10,0L0,5');

        // Start arrow
        defs.append('marker')
                .attr('id', 'start-arrow')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrow')
                .append('path')
                .attr('d', 'M10,-5L0,0L10,5');

        // Start arrow selected
        defs.append('marker')
                .attr('id', 'start-arrow-selected')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrowselected')
                .append('path')
                .attr('d', 'M10,-5L0,0L10,5');
    }
    function changeColor(nodeColor, jsonData) {

        var color = nodeColor;
        tree = d3.layout.tree().size([height, width]);
        var maxTreeWidth = breadthFirstTraversal(tree.nodes(jsonData), function (currentLevel) {

            currentLevel.forEach(function (node) {
                if (depthSelector == undefined || depthSelector == null)//25422
                    node.color = '#AE7A9B';
                if ((depthSelector == node['depth']) && depthSelector == 1)
                    node.color = nodeColor;
                if ((depthSelector == node['depth']) && depthSelector == 2)
                    node.color = nodeColor;
                if ((depthSelector == node['depth']) && depthSelector == 3)
                    node.color = nodeColor;
            });
        });
    }

    function deepCollapse(d) {
        d['children'].forEach((childObj) => {
            if (childObj.children) {
                childObj['children'].forEach(collapse);
                if (childObj.hasOwnProperty('children') &&
                        childObj.children instanceof Array &&
                        childObj.children.length > 0) {
                    childObj._children = childObj.children;
                    childObj.children = null;
                } else {
                    console.log("no Child");
                }
            }
        });
    }
}






function gridTreeBoxes(urlService, jsonData) {
    var urlService_ = '';
    var treeInitialized = jsonData['init'];
    var minHeight = jsonData['minHeight'];
    if (minHeight == undefined) {
        minHeight = 50;
    }
    var minWidth = jsonData['minWidth'];
    if (minWidth == undefined) {
        minWidth = 450;
    }
    var nodeColor = jsonData['color'];
//    var depthSelector = parseInt($("#depthSelector").val());
    var depthSelector = jsonData['depthLevel'];
    var blue = '', //#337ab7',
            green = '', //'#5cb85c',
            yellow = '', //'#f0ad4e',
            blueText = '', //'#4ab1eb',
            purple = '';//'#9467bd';
    var hideChild = jsonData['collapse'];//21422


    var margin = {
        top: 0,
        right: 0,
        bottom: 200,
        left: 0
    },
            // Height and width are redefined later in function of the size of the tree
            // (after that the data are loaded)
            width = 1000 - margin.right - margin.left,
            height = 450 - margin.top - margin.bottom;

    var rectNode = {width: 120, height: 40, textMargin: 5},
            tooltip = {width: 150, height: 40, textMargin: 5};
    var i = 0,
            duration = 750,
            root;

    var mousedown; // Use to save temporarily 'mousedown.zoom' value
    var mouseWheel,
            mouseWheelName,
            isKeydownZoom = false;

    var tree;
    var baseSvg,
            svgGroup,
            nodeGroup, // If nodes are not grouped together, after a click the svg node will be set after his corresponding tooltip and will hide it
            nodeGroupTooltip,
            linkGroup,
            linkGroupToolTip,
            defs;

    init(urlService, jsonData);

    function collapse(d) {
        if (d.children) {
            d._children = d.children;
            d._children.forEach(collapse);
            d.children = null;
        }
    }
    function collapsePrevNode(root) {           //19422
        if (root['prevNode'] != undefined && root['prevNode'].children) {
            if (root['prevNode']) {
                root['prevNode']._children = root['prevNode'].children;
                root['prevNode']._children.forEach(collapsePrevNode);
                if (root['prevNode']._children) {
                    root['prevNode'].children = null;
                }
            }
        }
    }
    function collapsePrevParentNode(root) {           //19422
        if (root['parentPrevNode'] != undefined && root['parentPrevNode'].children) {
            if (root['parentPrevNode']) {
                root['parentPrevNode']._children = root['parentPrevNode'].children;
//                root['parentPrevNode']._children.forEach(collapsePrevNode);
                root['parentPrevNode'].children.forEach(function (d) {//29422
                    if (d.children) {
                        d._children = d.children;
                        d.children = null;
                    }
                });//29422
                root['parentPrevNode'].children = null;

                if (root['parentPrevNode']._children) {
                    root['parentPrevNode'] = null;
                }
            }
        }
    }

    function init(urlService, jsonData)
    {
        urlService_ = urlService;
        if (urlService && urlService.length > 0)
        {
            if (urlService.charAt(urlService.length - 1) != '/')
                urlService_ += '/';
        }

        if (jsonData)
            drawTree(jsonData);
        else
        {
            console.error(jsonData);
            alert('Invalides data.');
        }
    }

    function drawTree(jsonData)
    {
        tree = d3.layout.tree().size([height, width]);
        root = jsonData;
        root.fixed = true;

        // Dynamically set the height of the main svg container
        // breadthFirstTraversal returns the max number of node on a same level
        // and colors the nodes
        var maxDepth = 0;
        var maxTreeWidth = breadthFirstTraversal(tree.nodes(root), function (currentLevel) {
            maxDepth++;
            if (!hideChild || hideChild == undefined || hideChild == null)//27422
            {
                if (nodeColor != null && nodeColor != '' && nodeColor != undefined) {
                    currentLevel.forEach(function (node) {
                        if (depthSelector == undefined || depthSelector == null)
                            node.color = '#AE7A9B';
                    });
                }
            }
        });

        if (minHeight == undefined || minHeight == null) {
            height = maxTreeWidth * (rectNode.height + (-40)) + tooltip.height + (-40) - margin.right - margin.left;
            width = maxDepth * (rectNode.width * 2) + tooltip.width / 1 - margin.top - margin.bottom;
        }

        if (treeInitialized) {
            deepCollapse(jsonData);
        } else {
            jsonData['minHeight'] = height;
            jsonData['minWidth'] = width;
        }
        tree = d3.layout.tree().size([height, width]);
        root.x0 = height / 2;
        root.y0 = 0;
        if(root.children!= null && root.children!= undefined && root.children.length > 0){
                root.children.forEach(collapse);
        }
        function collapse(d) {
            if (d.children) {
                d._children = d.children;
                d._children.forEach(collapse);
                d.children = null;
            }
        }

        baseSvg = d3.select('#dxpDecomposeTreeClass').insert('svg', ":first-child")
                .attr('width', width + margin.right + margin.left)
                .attr('height', height + margin.top + margin.bottom)
                .attr('class', 'svgContainer')
                .call(d3.behavior.zoom()
                        //.scaleExtent([0.5, 1.5]) // Limit the zoom scale
                        .on('zoom', zoomAndDrag));

        // Mouse wheel is desactivated, else after a first drag of the tree, wheel event drags the tree (instead of scrolling the window)
        getMouseWheelEvent();
        d3.select('#dxpDecomposeTreeClass').select('svg').on(mouseWheelName, null);
        d3.select('#dxpDecomposeTreeClass').select('svg').on('dblclick.zoom', null);

        svgGroup = baseSvg.append('g')
                .attr('class', 'drawarea')
                .append('g')
                .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

        // SVG elements under nodeGroupTooltip could be associated with nodeGroup,
        // same for linkGroupToolTip and linkGroup,
        // but this separation allows to manage the order on which elements are drew
        // and so tooltips are always on top.
        nodeGroup = svgGroup.append('g')
                .attr('id', 'nodes');
        linkGroup = svgGroup.append('g')
                .attr('id', 'links');
        linkGroupToolTip = svgGroup.append('g')
                .attr('id', 'linksTooltips');
        nodeGroupTooltip = svgGroup.append('g')
                .attr('id', 'nodesTooltips');

        defs = baseSvg.append('defs');
        initArrowDef();
        initDropShadow();

        update(root);
    }

    function update(source) {
        var nodes = tree.nodes(root).reverse(),
                links = tree.links(nodes);

        // Check if two nodes are in collision on the ordinates axe and move them
        breadthFirstTraversal(tree.nodes(root), collision);
        // Normalize for fixed-depth
        nodes.forEach(function (d) {
            d.y = d.depth * (rectNode.width * 1.5);
        });

        // 1) ******************* Update the nodes *******************
        var node = nodeGroup.selectAll('g.node').data(nodes, function (d) {
            return d.id || (d.id = ++i);
        });
        var nodesTooltip = nodeGroupTooltip.selectAll('g').data(nodes, function (d) {
            return d.id || (d.id = ++i);
        });

        // Enter any new nodes at the parent's previous position
        // We use "insert" rather than "append", so when a new child node is added (after a click)
        // it is added at the top of the group, so it is drawed first
        // else the nodes tooltips are drawed before their children nodes and they
        // hide them
        var nodeEnter = node.enter().insert('g', 'g.node')
                .attr('class', 'node')
                .style('stroke-width', '4px')
                .style('stroke', function (d) {
                    var myNodeName = d['nodeName'];
                    if (d["depth"] == 0 && myNodeName != null && myNodeName != undefined && myNodeName != '') {
                        return "#00acee";
                    }
                    if (myNodeName === 'TWITTER') {
                        return '#00acee';
                    } else if (myNodeName == 'YOUTUBE') {
                        return '#FF0000';
                    } else if (myNodeName == 'NEWS PAPER') {
                        return '#4169E1';
                    } else if (myNodeName == 'News Channel') {
                        return '#0047AB';
                    } else if (myNodeName == 'JSP') {
                        return '#FF0000';
                    } else if (myNodeName == 'PUBLIC') {
                        return '#f3cea1'
                    } else if (myNodeName == 'null') {
                        return '#FF0000';
                    } else if (myNodeName == 'YSRCP') {
                        return '#0266b4';
                    } else if (myNodeName == 'AIMIM') {
                        return '#20783B'
                    } else if (myNodeName == 'BJP') {
                        return '#F97D09'
                    } else if (myNodeName == 'INC') {
                        return '#009900';
                    } else if (myNodeName == 'IND') {
                        return '#dc3066';
                    } else if (myNodeName == 'JSP') {
                        return '#da2824';
                    } else if (myNodeName == 'PILOG') {
                        return '#1DA1F2';
                    } else if (myNodeName == 'PUBLIC' || d['depth'] == 3 || d['depth'] == 4 || d['depth'] == 5 || d['depth'] == 6 || d['depth'] == 7) {
                        return '#f3cea1'
                    } else if (myNodeName == 'TDP') {
                        return '#e3b713';
                    } else if (myNodeName == 'TRS') {
                        return '#FE0175';
                    } else if (myNodeName == 'YSRCP') {
                        return '#0266b4';
                    } else if (myNodeName == 'OTHERS') {
                        return '#f3cea1';
                    } else {
                        return  'hsla(' + (Math.random() * 360) + ', 100%, 50%, 1)';

                    }
                }
                )
                .attr('transform', function (d) {
                    return 'translate(' + d.y + ',' + d.x + ')';
                })
                .on('click', function (d) {
                    if (!d._children) {
                        getLevelWiseTreeData(d);
                    } else {
                        click(d);
                    }
                });
        var nodeEnterTooltip = nodesTooltip.enter().append('g')
                .attr('transform', function (d) {
                    return 'translate(' + d.y + ',' + d.x + ')';
                });

        nodeEnter.append('g').append('rect')
                .attr('rx', 6)
                .attr('ry', 6)
                .attr('width', rectNode.width)
                .attr('height', rectNode.height)
                .attr('class', 'node-rect')
                .attr('nodeDepth', function (d) {

                    if (d["depth"] == 0)
                        return 'nodeDepthLevel0';
                    if (d["depth"] == 1)
                        return 'nodeDepthLevel1';
                    if (d["depth"] == 2)
                        return 'nodeDepthLevel2';
                    if (d["depth"] == 3)
                        return 'nodeDepthLevel3';
                    if (d["depth"] == 4)
                        return 'nodeDepthLevel4';
                })
                .attr('fill', function (d) {
                    var nodeName = d['name'];
//                    var parentData = d['parent'];
//                    var parentNode = d['parent']['nodeName'];
                    if (d["depth"] == 0 || d["depth"] == 1 || d["depth"] == 2 || d["depth"] == 3 || d["depth"] == 4 || d["depth"] == 5 || d["depth"] == 6) {
                        return "#FFFFFF";
                    }
//                    if (d["depth"] == 1 ){
//                        return "#FFFFFF";
//                    }
//                    if (d["depth"] == 1 && nodeName == 'News Channel' || nodeName == 'NEWS CHANNEL') {
//                        return "#A6DE87";
//                    } else if (d["depth"] == 1 && (nodeName == 'YOUTUBE' || nodeName == 'Youtube')) {
//                        return "#FF0000";
//                    } else if (d["depth"] == 1 && (nodeName == 'Twitter' || nodeName == 'TWITTER')) {
//                        return "#1DA1F2";
//                    } else if (d["depth"] == 1 && (nodeName == 'NEWS PAPER' || nodeName == 'News Paper')) {
//                        return "#F3CEA1";
//                    }
//                    if (d["depth"] == 2 && parentNode == 'News Channel' || parentNode == 'NEWS CHANNEL'){
//                        return "#A6DE87";
//                    } else if (d["depth"] == 2 && (parentNode == 'YOUTUBE' || parentNode == 'Youtube')){
//                        return "#FF0000";
//                    } else if (d["depth"] == 2 && (parentNode == 'Twitter' || parentNode == 'TWITTER')){
//                        return "#1DA1F2";
//                    } else if (d["depth"] == 2 && (parentNode == 'NEWS PAPER' || parentNode == 'News Paper')){
//                        return "#F3CEA1";
//                    }
//                    else if (d["depth"] == 2 && nodeName == 'TDP') {
//                        return "#E3B713";
//                    } else if (d["depth"] == 2 && (nodeName == 'YCP' || nodeName == 'YSRCP')) {
//                        return "#0266B4";
//                    } else if (d["depth"] == 2 && (nodeName == 'PUBLIC' || nodeName == 'OTHERS')) {
//                        return "#F3CEA1";
//                    } else if (d["depth"] == 2 && (nodeName == 'JSP' || nodeName == 'JANASENA')) {
//                        return "#DA2824";
//                    } else if (d["depth"] == 2 && (nodeName == 'TRS' || nodeName == 'Trs')) {
//                        return "#FE0175";
//
//                    } else if (d["depth"] == 2 && (nodeName == 'BJP' || nodeName == 'Bjp')) {
//                        return "#F97D09";
//                    } else if (d["depth"] == 2 && (nodeName == 'INC' || nodeName == 'Inc')) {
//                        return "#009900";
//
//                    } else if (d["depth"] == 2 && (nodeName == 'IND' || nodeName == 'Ind')) {
//                        return "#DC3066";
//
//                    } else if (d["depth"] == 2 && (nodeName == 'AIMIM' || nodeName == 'Aimim')) {
//                        return "#20783B";
//
//                    }
//                    else if (d["depth"] == 2 && (nodeName == 'PILOG' || nodeName == 'PiLog')) {
//                        return "#1DA1F2";
//
//                    }
//                    else if (d["depth"] == 2 && (nodeName == null || nodeName == 'undefined')) {
//                        return "#DA2824";
//                    } else if (d["depth"] == 3 && nodeName == 'TDP') {
//                        return "#E3B713";
//                    } else if (d["depth"] == 3 && nodeName == 'YCP') {
//                        return "#0266B4";
//                    } else if (d["depth"] == 3 && nodeName == 'PUBLIC') {
//                        return "#F3CEA1";
//                    } else if (d["depth"] == 3 && nodeName == 'JSP') {
//                        return "#DA2824";
//                    } else if (d["depth"] == 3 && nodeName == 'TRS') {
//                        return "#FE0175";
//                    } else if (d["depth"] == 3 && nodeName == 'BJP') {
//                        return "#F97D09";
//                    } else if (d["depth"] == 3 && nodeName == 'INC') {
//                        return "#009900";
//                    } else if (d["depth"] == 3 && nodeName == 'IND') {
//                        return "#DC3066";
//                    }
//                    else if (d["depth"] == 3 && (nodeName == 'AIMIM' || nodeName == 'Aimim')) {
//                        return "#20783B";
//
//                    }
//                    else if (d["depth"] == 3 && (nodeName == 'PILOG' || nodeName == 'PiLog')) {
//                        return "#1DA1F2";
//
//                    }
//                    else if (d["depth"] == 4 && nodeName == 'TDP') {
//                        return "#E3B713";
//                    } else if (d["depth"] == 4 && nodeName == 'YCP') {
//                        return "#0266B4";
//                    } else if (d["depth"] == 4 && nodeName == 'PUBLIC') {
//                        return "#F3CEA1";
//                    } else if (d["depth"] == 4 && nodeName == 'JSP') {
//                        return "#DA2824";
//                    } else if (d["depth"] == 4 && nodeName == 'TDP') {
//                        return "#E3B713";
//                    } else if (d["depth"] == 4 && nodeName == 'BJP') {
//                        return "#F97D09";
//                    } else if (d["depth"] == 4 && nodeName == 'INC') {
//                        return "#009900";
//                    } else if (d["depth"] == 4 && nodeName == 'IND') {
//                        return "#DC3066";
//                    }
//                    else if (d["depth"] == 4 && (nodeName == 'AIMIM' || nodeName == 'Aimim')) {
//                        return "#20783B";
//
//                    }
//                    else if (d["depth"] == 4 && (nodeName == 'PILOG' || nodeName == 'PiLog')) {
//                        return "#1DA1F2";
//
//                    }
//                    if (d["depth"] == 3)
//                        return "#AE7A9B";
//                    if (d["depth"] == 4)
//                        return "#AE7A9B";
                })
                .attr('filter', 'url(#drop-shadow)');

        nodeEnter.append('foreignObject')
                .attr('x', rectNode.textMargin)
                .attr('y', rectNode.textMargin)
                .attr('width', function () {
                    return (rectNode.width - rectNode.textMargin * 2) < 0 ? 0
                            : (rectNode.width - rectNode.textMargin * 2)
                })
                .attr('height', function () {
                    return (rectNode.height - rectNode.textMargin * 2) < 0 ? 0
                            : (rectNode.height - rectNode.textMargin * 2)
                })
                .append('xhtml').html(function (d) {
            var divStr = '';
            if (d["depth"] == 0) {
                if (d.count != null && d.count != '' && d.count != undefined)
                {
                    divStr = '<b><img src=' + d.icon + ' width="18px"/></b>' + d.count + '<br>'
                } else {
                    divStr = '';
                }
            } else if (d["depth"] == 3) {
                divStr = '<b><img src=' + d.icon + ' width="18px"/></b>' + d.count + '<br>'
            } else if (d["depth"] >= 4) {
                if (d.count != null && d.count != '' && d.count != undefined)
                {
                    divStr = '<b><img src=' + d.icon + ' width="18px"/></b>' + d.count + '<br>'
                } else if (d.nodeName.includes("Like")) {
                    divStr = '<b><img src=' + d.likeIcon + ' width="18px"/></b>';
                } else if (d.nodeName.includes("Views") || d.nodeName.includes("Retweet")) {
                    divStr = '<b><img src=' + d.viewsIcon + ' width="15px"/></b>';
                } else if (d.nodeName.includes("Dislike")) {
                    divStr = '<b><img src=' + d.disLikeIcon + ' width="18px"/></b>';
                } else if (d.nodeName.includes("Comments")) {
                    divStr = '<b><img src=' + d.commentIcon + ' width="18px"/></b>';
                } else if (d.nodeName.includes("Positive")) {
                    divStr = '<b><img src=' + d.positiveIcon + ' width="18px"/></b>';
                } else if (d.nodeName.includes("Negative")) {
                    divStr = '<b><img src=' + d.negativeIcon + ' width="18px"/></b>';
                } else if (d.nodeName.includes("Neutral")) {
                    divStr = '<b><img src=' + d.neutralIcon + ' width="18px"/></b>';
                } else {
                    divStr = '';
                }
            } else {
                if (d.count != null && d.count != '' && d.count != undefined)
                {
                    divStr = '<b><img src=' + d.icon + ' width="18px"/></b>' + d.count + '<br>'
                } else {
                    divStr = '';
                }
            }
            return '<div style="width: '
                    + (rectNode.width - rectNode.textMargin * 2) + 'px; height: '
                    + (rectNode.height - rectNode.textMargin * 2) + 'px;" class="node-text wordwrap">'
                    + '<b>' + d.nodeName + '</b><br>'
                    + '' + divStr + ''
//                    + '<b>Instance: </b>' + d.instance + '<br>'
                    + '</div>';
        })
                .on('mouseover', function (d) {
                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
                })
                .on('mouseout', function (d) {
                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
                });

//        nodeEnterTooltip.append("rect")
//                .attr('id', function (d) {
//                    return 'nodeInfoID' + d.id;
//                })
//                .attr('x', rectNode.width / 2)
//                .attr('y', rectNode.height / 2)
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .attr('class', 'tooltip-box')
//                .style('fill-opacity', 0.8)
//                .on('mouseover', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
//                    removeMouseEvents();
//                })
//                .on('mouseout', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
//                    reactivateMouseEvents();
//                });

//        nodeEnterTooltip.append("text")
//                .attr('id', function (d) {
//                    return 'nodeInfoTextID' + d.id;
//                })
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('y', rectNode.height / 2 + tooltip.textMargin * 2)
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .attr('class', 'tooltip-text')
//                .style('fill', 'white')
//                .append("tspan")
//                .text(function (d) {
//                    return 'Name: ' + d.name;
//                })
//                .append("tspan")
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return '';
//                })
//                .append("tspan")
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return '';
//                });

        // Transition nodes to their new position.
        var nodeUpdate = node.transition().duration(duration)
                .attr('transform', function (d) {
                    return 'translate(' + d.y + ',' + d.x + ')';
                });
//        nodesTooltip.transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + d.y + ',' + d.x + ')';
//                });

        nodeUpdate.select('rect')
                .attr('class', function (d) {
                    return d._children ? 'node-rect-closed' : 'node-rect';
                });

        nodeUpdate.select('text').style('fill-opacity', 1);

        // Transition exiting nodes to the parent's new position
        var nodeExit = node.exit().transition().duration(duration)
                .attr('transform', function (d) {
                    return 'translate(' + d.y + ',' + d.x + ')';
                })
                .remove();
//        nodesTooltip.exit().transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y + ',' + source.x + ')';
//                })
//                .remove();

        nodeExit.select('text').style('fill-opacity', 1e-6);


        // 2) ******************* Update the links *******************
        var link = linkGroup.selectAll('path').data(links, function (d) {
            return d.target.id;
        });
        var linkTooltip = linkGroupToolTip.selectAll('g').data(links, function (d) {
            return d.target.id;
        });

        function linkMarkerStart(direction, isSelected) {
            if (direction == 'SYNC')
            {
                return isSelected ? 'url(#start-arrow-selected)' : 'url(#start-arrow)';
            }
            return '';
        }

//        function linkType(link) {
//            if (link.direction == 'SYNC')
//                return "Synchronous [\u2194]";
//            else
//            {
//                if (link.direction == 'ASYN')
//                    return "Asynchronous [\u2192]";
//            }
//            return '???';
//        }

        d3.selection.prototype.moveToFront = function () {
            return this.each(function () {
                this.parentNode.appendChild(this);
            });
        };

        // Enter any new links at the parent's previous position.
        // Enter any new links at the parent's previous position.
        var linkenter = link.enter().insert('path', 'g')
                .attr('class', 'link')
                .attr('id', function (d) {
                    return 'linkID' + d.target.id;
                })
                .attr('d', function (d) {
                    return diagonal(d);
                })
                .attr('marker-end', 'url(#end-arrow)')
                .attr('marker-start', function (d) {
                    return linkMarkerStart(d.target.link.direction, false);
                })
                .attr('marker-start', function (d) {
                    return linkMarkerStart(d.target.link.direction, false);
                })
//                .on('mouseover', function (d) {
//                    d3.select(this).moveToFront();
//
//                    d3.select(this).attr('marker-end', 'url(#end-arrow-selected)');
//                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, true));
//                    d3.select(this).attr('class', 'linkselected');
//
//                    $('#tooltipLinkID' + d.target.id).attr('x', (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y);
//                    $('#tooltipLinkID' + d.target.id).attr('y', (d.target.x - d.source.x) / 2 + d.source.x);
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
//                })
//                .on('mouseout', function (d) {
//                    d3.select(this).attr('marker-end', 'url(#end-arrow)');
//                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, false));
//                    d3.select(this).attr('class', 'link');
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
//                });

//        linkTooltip.enter().append('rect')
//                .attr('id', function (d) {
//                    return 'tooltipLinkID' + d.target.id;
//                })
//                .attr('class', 'tooltip-box')
//                .style('fill-opacity', 0.8)
                .attr('x', function (d) {
                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y;
                })
                .attr('y', function (d) {
                    return (d.target.x - d.source.x) / 2 + d.source.x;
                })
                .attr('width', tooltip.width)
                .attr('height', tooltip.height)
//                .on('mouseover', function (d) {
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
//                    // After selected a link, the cursor can be hover the tooltip, that's why we still need to highlight the link and the arrow
//                    $('#linkID' + d.target.id).attr('class', 'linkselected');
//                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow-selected)');
//                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, true));
//
//                    removeMouseEvents();
//                })
//                .on('mouseout', function (d) {
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
//                    $('#linkID' + d.target.id).attr('class', 'link');
//                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow)');
//                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, false));
//
//                    reactivateMouseEvents();
//                });

//        linkTooltip.enter().append('text')
//                .attr('id', function (d) {
//                    return 'tooltipLinkTextID' + d.target.id;
//                })
//                .attr('class', 'tooltip-text')
                .attr('x', function (d) {
                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
                })
                .attr('y', function (d) {
                    return (d.target.x - d.source.x) / 2 + d.source.x + tooltip.textMargin * 2;
                })
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .style('fill', 'white')
////                .append("tspan")
////                .text(function (d) {
////                 //   return linkType(d.target.link);
////                })
                .append("tspan")
                .attr('x', function (d) {
                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
                })
                .attr('dy', '1.5em')
                .text(function (d) {
                    return d.target.link.name;
                });

        // Transition links to their new position.
        var linkUpdate = link.transition().duration(duration)
                .attr('d', function (d) {
                    return diagonal(d);
                });
        linkTooltip.transition().duration(duration)
                .attr('d', function (d) {
                    return diagonal(d);
                });

        // Transition exiting nodes to the parent's new position.
        link.exit().transition()
                .remove();

        linkTooltip.exit().transition()
                .remove();

        // Stash the old positions for transition.
        nodes.forEach(function (d) {
            d.x0 = d.x;
            d.y0 = d.y;
        });
    }

    // Zoom functionnality is desactivated (user can use browser Ctrl + mouse wheel shortcut)
    function zoomAndDrag() {
        //var scale = d3.event.scale,
        var scale = 1,
                translation = d3.event.translate,
                tbound = -height * scale,
                bbound = height * scale,
                lbound = (-width + margin.right) * scale,
                rbound = (width - margin.left) * scale;
        // limit translation to thresholds
        translation = [
            Math.max(Math.min(translation[0], rbound), lbound),
            Math.max(Math.min(translation[1], bbound), tbound)
        ];
        d3.select('.drawarea')
                .attr('transform', 'translate(' + translation + ')' +
                        ' scale(' + scale + ')');
    }

    // Toggle children on click.
    function click(d) {
        if (d['children'] || d['_children']) {
            if (d.children) {
                d._children = d.children;
                d.children = null;
            } else {
                d.children = d._children;
                d._children = null;
                var nodeDepth = d.depth;
                if (root['prevDepth' + nodeDepth] && root['prevDepth' + nodeDepth] != undefined && root['prevDepth' + nodeDepth] != null) {
                    if (d.id != root['prevDepth' + nodeDepth]['id'])
                        collapseDepth(root['prevDepth' + nodeDepth], nodeDepth);
                }
                root['prevDepth' + nodeDepth] = d;

            }
            update(d);
        }
    }
    //17522
    function collapseDepth(d, x) {
        if (d.children) {
            d._children = d.children;
            d.children.forEach(function (d) {
                if (d.children) {
                    d._children = d.children;
                    d.children = null;
                }
            });
            d.children = null;
        }
    }

    // Breadth-first traversal of the tree
    // func function is processed on every node of a same level
    // return the max level
    function breadthFirstTraversal(tree, func)
    {
        var max = 0;
        if (tree && tree.length > 0)
        {
            var currentDepth = tree[0].depth;
            var fifo = [];
            var currentLevel = [];

            fifo.push(tree[0]);
            while (fifo.length > 0) {
                var node = fifo.shift();
                if (node.depth > currentDepth) {
                    func(currentLevel);
                    currentDepth++;
                    max = Math.max(max, currentLevel.length);
                    currentLevel = [];
                }
                currentLevel.push(node);
                if (node.children) {
                    for (var j = 0; j < node.children.length; j++) {
                        fifo.push(node.children[j]);
                    }
                }
            }
            func(currentLevel);
            return Math.max(max, currentLevel.length);
        }
        return 0;
    }

    // x = ordoninates and y = abscissas
    function collision(siblings) {
        var minPadding = 5;
        if (siblings) {
            for (var i = 0; i < siblings.length - 1; i++)
            {
                if (siblings[i + 1].x - (siblings[i].x + rectNode.height) < minPadding)
                    siblings[i + 1].x = siblings[i].x + rectNode.height + minPadding;
            }
        }
    }

    function removeMouseEvents() {
        // Drag and zoom behaviors are temporarily disabled, so tooltip text can be selected
        mousedown = d3.select('#dxpDecomposeTreeClass').select('svg').on('mousedown.zoom');
        d3.select('#dxpDecomposeTreeClass').select('svg').on("mousedown.zoom", null);
    }

    function reactivateMouseEvents() {
        // Reactivate the drag and zoom behaviors
        d3.select('#dxpDecomposeTreeClass').select('svg').on('mousedown.zoom', mousedown);
    }

    // Name of the event depends of the browser
    function getMouseWheelEvent() {
        if (d3.select('#dxpDecomposeTreeClass').select('svg').on('wheel.zoom'))
        {
            mouseWheelName = 'wheel.zoom';
            return d3.select('#dxpDecomposeTreeClass').select('svg').on('wheel.zoom');
        }
        if (d3.select('#dxpDecomposeTreeClass').select('svg').on('mousewheel.zoom') != null)
        {
            mouseWheelName = 'mousewheel.zoom';
            return d3.select('#dxpDecomposeTreeClass').select('svg').on('mousewheel.zoom');
        }
        if (d3.select('#dxpDecomposeTreeClass').select('svg').on('DOMMouseScroll.zoom'))
        {
            mouseWheelName = 'DOMMouseScroll.zoom';
            return d3.select('#dxpDecomposeTreeClass').select('svg').on('DOMMouseScroll.zoom');
        }
    }

    function diagonal(d) {
        var p0 = {
            x: d.source.x + rectNode.height / 2,
            y: (d.source.y + rectNode.width)
        }, p3 = {
            x: d.target.x + rectNode.height / 2,
            y: d.target.y - 12 // -12, so the end arrows are just before the rect node
        }, m = (p0.y + p3.y) / 2, p = [p0, {
                x: p0.x,
                y: m
            }, {
                x: p3.x,
                y: m
            }, p3];
        p = p.map(function (d) {
            return [d.y, d.x];
        });
        return 'M' + p[0] + 'C' + p[1] + ' ' + p[2] + ' ' + p[3];
    }

    function initDropShadow() {
        var filter = defs.append("filter")
                .attr("id", "drop-shadow")
                .attr("color-interpolation-filters", "sRGB");

        filter.append("feOffset")
                .attr("result", "offOut")
                .attr("in", "SourceGraphic")
                .attr("dx", 0)
                .attr("dy", 0);

        filter.append("feGaussianBlur")
                .attr("stdDeviation", 2);

        filter.append("feOffset")
                .attr("dx", 2)
                .attr("dy", 2)
                .attr("result", "shadow");

        filter.append("feComposite")
                .attr("in", 'offOut')
                .attr("in2", 'shadow')
                .attr("operator", "over");
    }

    function initArrowDef() {
        // Build the arrows definitions
        // End arrow
        defs.append('marker')
                .attr('id', 'end-arrow')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrow')
                .append('path')
                .attr('d', 'M0,-5L10,0L0,5');

        // End arrow selected
        defs.append('marker')
                .attr('id', 'end-arrow-selected')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrowselected')
                .append('path')
                .attr('d', 'M0,-5L10,0L0,5');

        // Start arrow
        defs.append('marker')
                .attr('id', 'start-arrow')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrow')
                .append('path')
                .attr('d', 'M10,-5L0,0L10,5');

        // Start arrow selected
        defs.append('marker')
                .attr('id', 'start-arrow-selected')
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 0)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .attr('class', 'arrowselected')
                .append('path')
                .attr('d', 'M10,-5L0,0L10,5');
    }
    function changeColor(nodeColor, jsonData) {

        var color = nodeColor;
        tree = d3.layout.tree().size([height, width]);
        var maxTreeWidth = breadthFirstTraversal(tree.nodes(jsonData), function (currentLevel) {

            currentLevel.forEach(function (node) {
                if (depthSelector == undefined || depthSelector == null)//25422
                    node.color = '#AE7A9B';
                if ((depthSelector == node['depth']) && depthSelector == 1)
                    node.color = nodeColor;
                if ((depthSelector == node['depth']) && depthSelector == 2)
                    node.color = nodeColor;
                if ((depthSelector == node['depth']) && depthSelector == 3)
                    node.color = nodeColor;
            });
        });
    }

    function deepCollapse(d) {
        d['children'].forEach((childObj) => {
            if (childObj.children) {
                childObj['children'].forEach(collapse);
                if (childObj.hasOwnProperty('children') &&
                        childObj.children instanceof Array &&
                        childObj.children.length > 0) {
                    childObj._children = childObj.children;
                    childObj.children = null;
                } else {
                    console.log("no Child");
                }
            }
        });
    }


    function getLevelWiseTreeData(d)
    {
        var level = d.level;
        var name = d.name;
        var hashTag = d.hashTag;
        $("#level" + level).remove();
        var levelWhere = d.levelWhereCond;
        $("body").append(levelWhere);
        var levelWhereCond = $("#level" + level).val();
        level = parseInt(level);
        level++;
        var checkedVal = $("#notifictionDecomRadioBtn input:checked").val();
        var paramObj = {};
        var fromToDate = "";
        if ($("#reportrange").length > 0)
        {
            fromToDate = $("#reportrange").find("span").html();
        } else {
            fromToDate = $("#dxpDecomposeTreeDatePickerId").find("span").html();
        }
        if (fromToDate != null && fromToDate != '' && fromToDate != undefined) {
            var fromToDateArr = fromToDate.split("-");
            var fromd = new Date(fromToDateArr[0]);
            var fromDateStr = $.datepicker.formatDate('dd-mm-yy', fromd);
            var tod = new Date(fromToDateArr[1]);
            var toDateStr = $.datepicker.formatDate('dd-mm-yy', tod);
            paramObj.fromDate = fromDateStr;
            paramObj.toDate = toDateStr;
        }
        var hashTags = [];
        var searchedValue = $("#knowpulseInputSearch").val();
        if ($("#hashTagListData").length > 0)
        {
            var hashTagNames = $("#hashTagListData").jqxDropDownList('getCheckedItems');
            if (hashTagNames != null && !jQuery.isEmptyObject(hashTagNames))
            {
                $.each(hashTagNames, function (index) {
                    hashTags.push(this.value);
                });
            }
        }

        var topicKeyword = $("#topicKeyword").val();
        showLoader();
        $.ajax({
            url: 'getDecompositionChildrenTreeBox',
            type: "POST",
            dataType: 'json',
            traditional: true,
            cache: false,
            async: true,
            data: {
//                hashTag: hashTag,
                level: level,
                levelWhereCond: levelWhereCond,
//                checkedVal: checkedVal,
//                name: name,
//                paramObj: (paramObj != null && !jQuery.isEmptyObject(paramObj)) ? JSON.stringify(paramObj) : "",
//                topicKeyword: topicKeyword,
//                hashTags: (hashTags != null && !jQuery.isEmptyObject(hashTags)) ? JSON.stringify(hashTags) : "",
//                searchedValue: searchedValue,
            },
            success: function (response) {
                stopLoader();
                if (response != null && !jQuery.isEmptyObject(response))
                {
                    var result = response['TreeBoxObj'];
                    if (result != null && !jQuery.isEmptyObject(result))
                    {
                        var height = result.length * 42;
                        var svgHeight = $("#dxpDecomposeTreeClass").find("svg").attr("height");
                        if (height > parseInt(svgHeight)) {
                            $("#dxpDecomposeTreeClass").find("svg").attr("height", height + 200);
                        }
                        $(".homeTreeCardDiv").css("overflow-y", "auto");
                        var svgWidth = $("#dxpDecomposeTreeClass").find("svg").attr("width");
                        if (svgWidth != null && svgWidth != '' && svgWidth != undefined)
                        {
                            if (level >= 4)
                            {
                                svgWidth = parseInt(svgWidth) + 200;
                                $("#dxpDecomposeTreeClass").find("svg").attr("width", svgWidth);
                            }
                        }
                        result.forEach(function (i) {
                            if (d._children) {
                                d._children.push(i)
                            } else {
                                d['_children'] = [];
                                d['_children'].push(i);
                            }
                        });
                        setTimeout(function () {
                            click(d);
                        }, 1000);

                    }
                }
            }
        });
    }
}



///*
// * Dependencies:
// * - d3.js
// * - jquery.js
// */
//"use strict";
//
//function treeBoxes(urlService, jsonData){
//    var urlService_ = '';
//    var treeInitialized = jsonData['init'];
//    var minHeight = jsonData['minHeight'];
//    var minWidth = jsonData['minWidth'];
//    var nodeColor = jsonData['color'];
////    var depthSelector = parseInt($("#depthSelector").val());
//    var depthSelector = jsonData['depthLevel'];
//    var blue = '', //#337ab7',
//            green = '', //'#5cb85c',
//            yellow = '', //'#f0ad4e',
//            blueText = '', //'#4ab1eb',
//            purple = '';//'#9467bd';
//    var hideChild = jsonData['collapse'];//21422
//
//    var margin = {
//        top: 0,
//        right: 0,
//        bottom: 200,
//        left: 0
//    },
//            // Height and width are redefined later in function of the size of the tree
//            // (after that the data are loaded)
//            width = 1000 - margin.right - margin.left,
//            height = 800 - margin.top - margin.bottom;
//
//    var rectNode = {width: 120, height: 45, textMargin: 5},
//            tooltip = {width: 150, height: 40, textMargin: 5};
//    var i = 0,
//            duration = 750,
//            root;
//
//    var mousedown; // Use to save temporarily 'mousedown.zoom' value
//    var mouseWheel,
//            mouseWheelName,
//            isKeydownZoom = false;
//
//    var tree;
//    var baseSvg,
//            svgGroup,
//            nodeGroup, // If nodes are not grouped together, after a click the svg node will be set after his corresponding tooltip and will hide it
//            nodeGroupTooltip,
//            linkGroup,
//            linkGroupToolTip,
//            defs;
//
//    init(urlService, jsonData);
//
//    function collapse(d) {           //som
//        if (d.children) {
//            d._children = d.children;
//            d._children.forEach(collapse);
//            d.children = null;
//        }
//    }
//    function collapsePrevNode(root) {           //19422
//        if (root['prevNode'] != undefined && root['prevNode'].children) {
//            if (root['prevNode']) {
//                root['prevNode']._children = root['prevNode'].children;
//                root['prevNode']._children.forEach(collapsePrevNode);
//                if (root['prevNode']._children) {
//                    root['prevNode'].children = null;
//                }
//            }
//        }
//    }
//    function collapsePrevParentNode(root) {           //19422
//        if (root['parentPrevNode'] != undefined && root['parentPrevNode'].children) {
//            if (root['parentPrevNode']) {
//                root['parentPrevNode']._children = root['parentPrevNode'].children;
////                root['parentPrevNode']._children.forEach(collapsePrevNode);
//                root['parentPrevNode'].children.forEach(function (d) {//29422
//                    if (d.children) {
//                        d._children = d.children;
//                        d.children = null;
//                    }
//                });//29422
//                root['parentPrevNode'].children = null;
//
//                if (root['parentPrevNode']._children) {
//                    root['parentPrevNode'] = null;
//                }
//            }
//        }
//    }
//
//    function init(urlService, jsonData)
//    {
//        urlService_ = urlService;
//        if (urlService && urlService.length > 0)
//        {
//            if (urlService.charAt(urlService.length - 1) != '/')
//                urlService_ += '/';
//        }
//
//        if (jsonData)
//            drawTree(jsonData);
//        else
//        {
//            console.error(jsonData);
//            alert('Invalides data.');
//        }
//    }
//
//    function drawTree(jsonData)
//    {
//        tree = d3.layout.tree().size([height, width]);
//        root = jsonData;
//        root.fixed = true;
//
//        // Dynamically set the height of the main svg container
//        // breadthFirstTraversal returns the max number of node on a same level
//        // and colors the nodes
//        var maxDepth = 0;
//        var maxTreeWidth = breadthFirstTraversal(tree.nodes(root), function (currentLevel) {
//            maxDepth++;
//            if (!hideChild || hideChild == undefined || hideChild == null)//27422
//            {
//                if (nodeColor != null && nodeColor != '' && nodeColor != undefined) {
//                    currentLevel.forEach(function (node) {
//                        if (depthSelector == undefined || depthSelector == null)
//                            node.color = '#AE7A9B';
//                    });
//                }
//            }
//        });
//
//        if (minHeight == undefined || minHeight == null) {
//            height = maxTreeWidth * (rectNode.height + (-40)) + tooltip.height + (-40) - margin.right - margin.left;
//            width = maxDepth * (rectNode.width * 2) + tooltip.width / 1 - margin.top - margin.bottom;
//        }
//
//        if (treeInitialized){
//            deepCollapse(jsonData);
//        } else {
//            jsonData['minHeight'] = height;
//            jsonData['minWidth'] = width;
//        }
//        tree = d3.layout.tree().size([height, width]);
//        root.x0 = height / 2;
//        root.y0 = 0;
//
//        baseSvg = d3.select('#tree-container').append('svg')
//                .attr('width', width + margin.right + margin.left)
//                .attr('height', height + margin.top + margin.bottom)
//                .attr('class', 'svgContainer')
//                .call(d3.behavior.zoom()
//                        //.scaleExtent([0.5, 1.5]) // Limit the zoom scale
//                        .on('zoom', zoomAndDrag));
//
//        // Mouse wheel is desactivated, else after a first drag of the tree, wheel event drags the tree (instead of scrolling the window)
//        getMouseWheelEvent();
//        d3.select('#tree-container').select('svg').on(mouseWheelName, null);
//        d3.select('#tree-container').select('svg').on('dblclick.zoom', null);
//
//        svgGroup = baseSvg.append('g')
//                .attr('class', 'drawarea')
//                .append('g')
//                .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
//        nodeGroup = svgGroup.append('g')
//                .attr('id', 'nodes');
//        linkGroup = svgGroup.append('g')
//                .attr('id', 'links');
//        linkGroupToolTip = svgGroup.append('g')
//                .attr('id', 'linksTooltips');
//        nodeGroupTooltip = svgGroup.append('g')
//                .attr('id', 'nodesTooltips');
//
//        defs = baseSvg.append('defs');
//        initArrowDef();
//        initDropShadow();
//
//        update(root);
//    }
//
//    function update(source) {
//        var nodes = tree.nodes(root).reverse(),
//                links = tree.links(nodes);
//
//        // Check if two nodes are in collision on the ordinates axe and move them
//        breadthFirstTraversal(tree.nodes(root), collision);
//        // Normalize for fixed-depth
//        nodes.forEach(function (d) {
//            d.y = d.depth * (rectNode.width * 1.5);
//        });
//
//        // 1) ******************* Update the nodes *******************
//        var node = nodeGroup.selectAll('g.node').data(nodes, function (d) {
//            return d.id || (d.id = ++i);
//        });
//        var nodesTooltip = nodeGroupTooltip.selectAll('g').data(nodes, function (d) {
//            return d.id || (d.id = ++i);
//        });
//        var nodeEnter = node.enter().insert('g', 'g.node')
//                .attr('class', 'node')
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y0 + ',' + source.x0 + ')';
//                })
//                .on('click', function (d) {
//                    click(d);
//                });
//        var nodeEnterTooltip = nodesTooltip.enter().append('g')
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y0 + ',' + source.x0 + ')';
//                });
//
//        nodeEnter.append('g').append('rect')
//                .attr('rx', 6)
//                .attr('ry', 6)
//                .attr('width', rectNode.width)
//                .attr('height', rectNode.height)
//                .attr('class', 'node-rect')
//                .attr('nodeDepth', function (d) {
//
//                    if (d["depth"] == 0)
//                        return 'nodeDepthLevel0';
//                    if (d["depth"] == 1)
//                        return 'nodeDepthLevel1';
//                    if (d["depth"] == 2)
//                        return 'nodeDepthLevel2';
//                    if (d["depth"] == 3)
//                        return 'nodeDepthLevel3';
//                    if (d["depth"] == 4)
//                        return 'nodeDepthLevel4';
//                })
//                .attr('fill', function (d) {
//                    if (d["depth"] == 0)
//                        return $("#colorAdderlevel0").val();
//                    if (d["depth"] == 1)
//                        return $("#colorAdderlevel1").val();
//                    if (d["depth"] == 2)
//                        return $("#colorAdderlevel2").val();
//                    if (d["depth"] == 3)
//                        return $("#colorAdderlevel3").val();
//                    if (d["depth"] == 4)
//                        return $("#colorAdderlevel4").val();
//                })
//                .attr('filter', 'url(#drop-shadow)');
//
//        nodeEnter.append('foreignObject')
//                .attr('x', rectNode.textMargin)
//                .attr('y', rectNode.textMargin)
//                .attr('width', function () {
//                    return (rectNode.width - rectNode.textMargin * 2) < 0 ? 0
//                            : (rectNode.width - rectNode.textMargin * 2)
//                })
//                .attr('height', function () {
//                    return (rectNode.height - rectNode.textMargin * 2) < 0 ? 0
//                            : (rectNode.height - rectNode.textMargin * 2)
//                })
//                .append('xhtml').html(function (d) {
//            return '<div style="width: '
//                    + (rectNode.width - rectNode.textMargin * 2) + 'px; height: '
//                    + (rectNode.height - rectNode.textMargin * 2) + 'px;" class="node-text wordwrap">'
//                    + '<b>' + d.nodeName + '</b><br><br>'
////                    + '<b>Plant: </b>' + d.plant + '<br>'
////                    + '<b>Instance: </b>' + d.instance + '<br>'
//                    + '</div>';
//        })
//                .on('mouseover', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
//                })
//                .on('mouseout', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
//                });
//
//        nodeEnterTooltip.append("rect")
//                .attr('id', function (d) {
//                    return 'nodeInfoID' + d.id;
//                })
//                .attr('x', rectNode.width / 2)
//                .attr('y', rectNode.height / 2)
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .attr('class', 'tooltip-box')
//                .style('fill-opacity', 0.8)
//                .on('mouseover', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
//                    removeMouseEvents();
//                })
//                .on('mouseout', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
//                    reactivateMouseEvents();
//                });
//
//        nodeEnterTooltip.append("text")
//                .attr('id', function (d) {
//                    return 'nodeInfoTextID' + d.id;
//                })
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('y', rectNode.height / 2 + tooltip.textMargin * 2)
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .attr('class', 'tooltip-text')
//                .style('fill', 'white')
//                .append("tspan")
//                .text(function (d) {
//                    return 'Name: ' + d.name;
//                })
//                .append("tspan")
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return 'Supply:  DOMESTIC & NON-DOMESTIC';
//                })
//                .append("tspan")
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return 'Demand: 18';
//                });
//
//        // Transition nodes to their new position.
//        var nodeUpdate = node.transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + d.y + ',' + d.x + ')';
//                });
//        nodesTooltip.transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + d.y + ',' + d.x + ')';
//                });
//
//        nodeUpdate.select('rect')
//                .attr('class', function (d) {
//                    return d._children ? 'node-rect-closed' : 'node-rect';
//                });
//
//        nodeUpdate.select('text').style('fill-opacity', 1);
//
//        // Transition exiting nodes to the parent's new position
//        var nodeExit = node.exit().transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y + ',' + source.x + ')';
//                })
//                .remove();
//        nodesTooltip.exit().transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y + ',' + source.x + ')';
//                })
//                .remove();
//
//        nodeExit.select('text').style('fill-opacity', 1e-6);
//
//
//        // 2) ******************* Update the links *******************
//        var link = linkGroup.selectAll('path').data(links, function (d) {
//            return d.target.id;
//        });
//        var linkTooltip = linkGroupToolTip.selectAll('g').data(links, function (d) {
//            return d.target.id;
//        });
//
//        function linkMarkerStart(direction, isSelected) {
//            if (direction == 'SYNC')
//            {
//                return isSelected ? 'url(#start-arrow-selected)' : 'url(#start-arrow)';
//            }
//            return '';
//        }
//
//        function linkType(link) {
//            if (link.direction == 'SYNC')
//                return "Synchronous [\u2194]";
//            else
//            {
//                if (link.direction == 'ASYN')
//                    return "Asynchronous [\u2192]";
//            }
//            return '???';
//        }
//
//        d3.selection.prototype.moveToFront = function () {
//            return this.each(function () {
//                this.parentNode.appendChild(this);
//            });
//        };
//
//        // Enter any new links at the parent's previous position.
//        // Enter any new links at the parent's previous position.
//        var linkenter = link.enter().insert('path', 'g')
//                .attr('class', 'link')
//                .attr('id', function (d) {
//                    return 'linkID' + d.target.id;
//                })
//                .attr('d', function (d) {
//                    return diagonal(d);
//                })
//                .attr('marker-end', 'url(#end-arrow)')
//                .attr('marker-start', function (d) {
//                    return linkMarkerStart(d.target.link.direction, false);
//                })
//                .on('mouseover', function (d) {
//                    d3.select(this).moveToFront();
//
//                    d3.select(this).attr('marker-end', 'url(#end-arrow-selected)');
//                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, true));
//                    d3.select(this).attr('class', 'linkselected');
//
//                    $('#tooltipLinkID' + d.target.id).attr('x', (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y);
//                    $('#tooltipLinkID' + d.target.id).attr('y', (d.target.x - d.source.x) / 2 + d.source.x);
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
//                })
//                .on('mouseout', function (d) {
//                    d3.select(this).attr('marker-end', 'url(#end-arrow)');
//                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, false));
//                    d3.select(this).attr('class', 'link');
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
//                });
//
//        linkTooltip.enter().append('rect')
//                .attr('id', function (d) {
//                    return 'tooltipLinkID' + d.target.id;
//                })
//                .attr('class', 'tooltip-box')
//                .style('fill-opacity', 0.8)
//                .attr('x', function (d) {
//                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y;
//                })
//                .attr('y', function (d) {
//                    return (d.target.x - d.source.x) / 2 + d.source.x;
//                })
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .on('mouseover', function (d) {
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
//                    // After selected a link, the cursor can be hover the tooltip, that's why we still need to highlight the link and the arrow
//                    $('#linkID' + d.target.id).attr('class', 'linkselected');
//                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow-selected)');
//                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, true));
//
//                    removeMouseEvents();
//                })
//                .on('mouseout', function (d) {
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
//                    $('#linkID' + d.target.id).attr('class', 'link');
//                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow)');
//                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, false));
//
//                    reactivateMouseEvents();
//                });
//
//        linkTooltip.enter().append('text')
//                .attr('id', function (d) {
//                    return 'tooltipLinkTextID' + d.target.id;
//                })
//                .attr('class', 'tooltip-text')
//                .attr('x', function (d) {
//                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
//                })
//                .attr('y', function (d) {
//                    return (d.target.x - d.source.x) / 2 + d.source.x + tooltip.textMargin * 2;
//                })
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .style('fill', 'white')
//                .append("tspan")
//                .text(function (d) {
//                    return linkType(d.target.link);
//                })
//                .append("tspan")
//                .attr('x', function (d) {
//                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
//                })
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return d.target.link.name;
//                });
//
//        // Transition links to their new position.
//        var linkUpdate = link.transition().duration(duration)
//                .attr('d', function (d) {
//                    return diagonal(d);
//                });
//        linkTooltip.transition().duration(duration)
//                .attr('d', function (d) {
//                    return diagonal(d);
//                });
//
//        // Transition exiting nodes to the parent's new position.
//        link.exit().transition()
//                .remove();
//
//        linkTooltip.exit().transition()
//                .remove();
//
//        // Stash the old positions for transition.
//        nodes.forEach(function (d) {
//            d.x0 = d.x;
//            d.y0 = d.y;
//        });
//    }
//
//    // Zoom functionnality is desactivated (user can use browser Ctrl + mouse wheel shortcut)
//    function zoomAndDrag() {
//        //var scale = d3.event.scale,
//        var scale = 1,
//                translation = d3.event.translate,
//                tbound = -height * scale,
//                bbound = height * scale,
//                lbound = (-width + margin.right) * scale,
//                rbound = (width - margin.left) * scale;
//        // limit translation to thresholds
//        translation = [
//            Math.max(Math.min(translation[0], rbound), lbound),
//            Math.max(Math.min(translation[1], bbound), tbound)
//        ];
//        d3.select('.drawarea')
//                .attr('transform', 'translate(' + translation + ')' +
//                        ' scale(' + scale + ')');
//    }
//
//    // Toggle children on click.
//function click(d) {
//        if (d['children'] || d['_children']) {
//            if (d.children) {
//                d._children = d.children;
//                d.children = null;
//            } else {
//                d.children = d._children;
//                d._children = null;
//                var nodeDepth = d.depth;
//                    if (root['prevDepth'+nodeDepth] && root['prevDepth'+nodeDepth] != undefined && root['prevDepth'+nodeDepth] != null) {
//                        if (d.id != root['prevDepth'+nodeDepth]['id'])
//                            collapseDepth(root['prevDepth'+nodeDepth], nodeDepth);
//                    }
//                    root['prevDepth'+nodeDepth] = d;
//
//            }
//            update(d);
//        }
//    }
//    //17522
//    function collapseDepth(d, x) {
//        if (d.children) {
//            d._children = d.children;
//            d.children.forEach(function (d) {
//                if (d.children) {
//                    d._children = d.children;
//                    d.children = null;
//                }
//            });
//            d.children = null;
//        }
//    }
//
//    // Breadth-first traversal of the tree
//    // func function is processed on every node of a same level
//    // return the max level
//    function breadthFirstTraversal(tree, func)
//    {
//        var max = 0;
//        if (tree && tree.length > 0)
//        {
//            var currentDepth = tree[0].depth;
//            var fifo = [];
//            var currentLevel = [];
//
//            fifo.push(tree[0]);
//            while (fifo.length > 0) {
//                var node = fifo.shift();
//                if (node.depth > currentDepth) {
//                    func(currentLevel);
//                    currentDepth++;
//                    max = Math.max(max, currentLevel.length);
//                    currentLevel = [];
//                }
//                currentLevel.push(node);
//                if (node.children) {
//                    for (var j = 0; j < node.children.length; j++) {
//                        fifo.push(node.children[j]);
//                    }
//                }
//            }
//            func(currentLevel);
//            return Math.max(max, currentLevel.length);
//        }
//        return 0;
//    }
//
//    // x = ordoninates and y = abscissas
//    function collision(siblings) {
//        var minPadding = 5;
//        if (siblings) {
//            for (var i = 0; i < siblings.length - 1; i++)
//            {
//                if (siblings[i + 1].x - (siblings[i].x + rectNode.height) < minPadding)
//                    siblings[i + 1].x = siblings[i].x + rectNode.height + minPadding;
//            }
//        }
//    }
//
//    function removeMouseEvents() {
//        // Drag and zoom behaviors are temporarily disabled, so tooltip text can be selected
//        mousedown = d3.select('#tree-container').select('svg').on('mousedown.zoom');
//        d3.select('#tree-container').select('svg').on("mousedown.zoom", null);
//    }
//
//    function reactivateMouseEvents() {
//        // Reactivate the drag and zoom behaviors
//        d3.select('#tree-container').select('svg').on('mousedown.zoom', mousedown);
//    }
//
//    // Name of the event depends of the browser
//    function getMouseWheelEvent() {
//        if (d3.select('#tree-container').select('svg').on('wheel.zoom'))
//        {
//            mouseWheelName = 'wheel.zoom';
//            return d3.select('#tree-container').select('svg').on('wheel.zoom');
//        }
//        if (d3.select('#tree-container').select('svg').on('mousewheel.zoom') != null)
//        {
//            mouseWheelName = 'mousewheel.zoom';
//            return d3.select('#tree-container').select('svg').on('mousewheel.zoom');
//        }
//        if (d3.select('#tree-container').select('svg').on('DOMMouseScroll.zoom'))
//        {
//            mouseWheelName = 'DOMMouseScroll.zoom';
//            return d3.select('#tree-container').select('svg').on('DOMMouseScroll.zoom');
//        }
//    }
//
//    function diagonal(d) {
//        var p0 = {
//            x: d.source.x + rectNode.height / 2,
//            y: (d.source.y + rectNode.width)
//        }, p3 = {
//            x: d.target.x + rectNode.height / 2,
//            y: d.target.y - 12 // -12, so the end arrows are just before the rect node
//        }, m = (p0.y + p3.y) / 2, p = [p0, {
//                x: p0.x,
//                y: m
//            }, {
//                x: p3.x,
//                y: m
//            }, p3];
//        p = p.map(function (d) {
//            return [d.y, d.x];
//        });
//        return 'M' + p[0] + 'C' + p[1] + ' ' + p[2] + ' ' + p[3];
//    }
//
//    function initDropShadow() {
//        var filter = defs.append("filter")
//                .attr("id", "drop-shadow")
//                .attr("color-interpolation-filters", "sRGB");
//
//        filter.append("feOffset")
//                .attr("result", "offOut")
//                .attr("in", "SourceGraphic")
//                .attr("dx", 0)
//                .attr("dy", 0);
//
//        filter.append("feGaussianBlur")
//                .attr("stdDeviation", 2);
//
//        filter.append("feOffset")
//                .attr("dx", 2)
//                .attr("dy", 2)
//                .attr("result", "shadow");
//
//        filter.append("feComposite")
//                .attr("in", 'offOut')
//                .attr("in2", 'shadow')
//                .attr("operator", "over");
//    }
//
//    function initArrowDef() {
//        // Build the arrows definitions
//        // End arrow
//        defs.append('marker')
//                .attr('id', 'end-arrow')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrow')
//                .append('path')
//                .attr('d', 'M0,-5L10,0L0,5');
//
//        // End arrow selected
//        defs.append('marker')
//                .attr('id', 'end-arrow-selected')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrowselected')
//                .append('path')
//                .attr('d', 'M0,-5L10,0L0,5');
//
//        // Start arrow
//        defs.append('marker')
//                .attr('id', 'start-arrow')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrow')
//                .append('path')
//                .attr('d', 'M10,-5L0,0L10,5');
//
//        // Start arrow selected
//        defs.append('marker')
//                .attr('id', 'start-arrow-selected')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrowselected')
//                .append('path')
//                .attr('d', 'M10,-5L0,0L10,5');
//    }
//    function changeColor(nodeColor, jsonData) {
//
//        var color = nodeColor;
//        tree = d3.layout.tree().size([height, width]);
//        var maxTreeWidth = breadthFirstTraversal(tree.nodes(jsonData), function (currentLevel) {
//
//            currentLevel.forEach(function (node) {
//                if (depthSelector == undefined || depthSelector == null)//25422
//                    node.color = '#AE7A9B';
//                if ((depthSelector == node['depth']) && depthSelector == 1)
//                    node.color = nodeColor;
//                if ((depthSelector == node['depth']) && depthSelector == 2)
//                    node.color = nodeColor;
//                if ((depthSelector == node['depth']) && depthSelector == 3)
//                    node.color = nodeColor;
//            });
//        });
//    }
//
//    function deepCollapse(d) {
//        d['children'].forEach((childObj) => {
//            if (childObj.children) {
//                childObj['children'].forEach(collapse);
//                if (childObj.hasOwnProperty('children') &&
//                        childObj.children instanceof Array &&
//                        childObj.children.length > 0) {
//                    childObj._children = childObj.children;
//                    childObj.children = null;
//                } else {
//                    console.log("no Child");
//                }
//            }
//        });
//    }
//}
//
//
//
//
//
//
//function gridTreeBoxes(urlService, jsonData){
//    var urlService_ = '';
//    var treeInitialized = jsonData['init'];
//    var minHeight = jsonData['minHeight'];
//    if(minHeight==undefined){
//        minHeight = 50;
//    }
//    var minWidth = jsonData['minWidth'];
//    if(minWidth==undefined){   //som
//        minWidth=450;
//    }
//    var nodeColor = jsonData['color'];
////    var depthSelector = parseInt($("#depthSelector").val());
//    var depthSelector = jsonData['depthLevel'];
//    var blue = '', //#337ab7',
//            green = '', //'#5cb85c',
//            yellow = '', //'#f0ad4e',
//            blueText = '', //'#4ab1eb',
//            purple = '';//'#9467bd';
//    var hideChild = jsonData['collapse'];//21422
//
//
//    var margin = {
//        top: 0,
//        right: 0,
//        bottom: 200,
//        left: 0
//    },
//            // Height and width are redefined later in function of the size of the tree
//            // (after that the data are loaded)
//            width = 1000 - margin.right - margin.left,
//            height = 800 - margin.top - margin.bottom;
//
//    var rectNode = {width: 120, height: 45, textMargin: 5},
//            tooltip = {width: 150, height: 40, textMargin: 5};
//    var i = 0,
//            duration = 750,
//            root;
//
//    var mousedown; // Use to save temporarily 'mousedown.zoom' value
//    var mouseWheel,
//            mouseWheelName,
//            isKeydownZoom = false;
//
//    var tree;
//    var baseSvg,
//            svgGroup,
//            nodeGroup, // If nodes are not grouped together, after a click the svg node will be set after his corresponding tooltip and will hide it
//            nodeGroupTooltip,
//            linkGroup,
//            linkGroupToolTip,
//            defs;
//
//    init(urlService, jsonData);
//
//    function collapse(d) {           //som
//        if (d.children) {
//            d._children = d.children;
//            d._children.forEach(collapse);
//            d.children = null;
//        }
//    }
//    function collapsePrevNode(root) {           //19422
//        if (root['prevNode'] != undefined && root['prevNode'].children) {
//            if (root['prevNode']) {
//                root['prevNode']._children = root['prevNode'].children;
//                root['prevNode']._children.forEach(collapsePrevNode);
//                if (root['prevNode']._children) {
//                    root['prevNode'].children = null;
//                }
//            }
//        }
//    }
//    function collapsePrevParentNode(root) {           //19422
//        if (root['parentPrevNode'] != undefined && root['parentPrevNode'].children) {
//            if (root['parentPrevNode']) {
//                root['parentPrevNode']._children = root['parentPrevNode'].children;
////                root['parentPrevNode']._children.forEach(collapsePrevNode);
//                root['parentPrevNode'].children.forEach(function (d) {//29422
//                    if (d.children) {
//                        d._children = d.children;
//                        d.children = null;
//                    }
//                });//29422
//                root['parentPrevNode'].children = null;
//
//                if (root['parentPrevNode']._children) {
//                    root['parentPrevNode'] = null;
//                }
//            }
//        }
//    }
//
//    function init(urlService, jsonData)
//    {
//        urlService_ = urlService;
//        if (urlService && urlService.length > 0)
//        {
//            if (urlService.charAt(urlService.length - 1) != '/')
//                urlService_ += '/';
//        }
//
//        if (jsonData)
//            drawTree(jsonData);
//        else
//        {
//            console.error(jsonData);
//            alert('Invalides data.');
//        }
//    }
//
//    function drawTree(jsonData)
//    {
//        tree = d3.layout.tree().size([height, width]);
//        root = jsonData;
//        root.fixed = true;
//
//        // Dynamically set the height of the main svg container
//        // breadthFirstTraversal returns the max number of node on a same level
//        // and colors the nodes
//        var maxDepth = 0;
//        var maxTreeWidth = breadthFirstTraversal(tree.nodes(root), function (currentLevel) {
//            maxDepth++;
//            if (!hideChild || hideChild == undefined || hideChild == null)//27422
//            {
//                if (nodeColor != null && nodeColor != '' && nodeColor != undefined) {
//                    currentLevel.forEach(function (node) {
//                        if (depthSelector == undefined || depthSelector == null)
//                            node.color = '#AE7A9B';
//                    });
//                }
//            }
//        });
//
//        if (minHeight == undefined || minHeight == null) {
//            height = maxTreeWidth * (rectNode.height + (-40)) + tooltip.height + (-40) - margin.right - margin.left;
//            width = maxDepth * (rectNode.width * 2) + tooltip.width / 1 - margin.top - margin.bottom;
//        }
//
//        if (treeInitialized){
//            deepCollapse(jsonData);
//        } else {
//            jsonData['minHeight'] = height;
//            jsonData['minWidth'] = width;
//        }
//        tree = d3.layout.tree().size([height, width]);
//        root.x0 = height / 2;
//        root.y0 = 0;
////        root.children.forEach(collapse);
////        function collapse(d) {
////            if (d.children) {
////                d._children = d.children;
////                d._children.forEach(collapse);
////                d.children = null;
////            }
////        }
//
//        baseSvg = d3.select('#dxpDecomposeTreeClass').insert('svg',":first-child")
//                .attr('width', width + margin.right + margin.left)
//                .attr('height', height + margin.top + margin.bottom)
//                .attr('class', 'svgContainer')
//                .call(d3.behavior.zoom()
//                        //.scaleExtent([0.5, 1.5]) // Limit the zoom scale
//                        .on('zoom', zoomAndDrag));
//
//        // Mouse wheel is desactivated, else after a first drag of the tree, wheel event drags the tree (instead of scrolling the window)
//        getMouseWheelEvent();
//        d3.select('#dxpDecomposeTreeClass').select('svg').on(mouseWheelName, null);
//        d3.select('#dxpDecomposeTreeClass').select('svg').on('dblclick.zoom', null);
//
//        svgGroup = baseSvg.append('g')
//                .attr('class', 'drawarea')
//                .append('g')
//                .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
//
//        // SVG elements under nodeGroupTooltip could be associated with nodeGroup,
//        // same for linkGroupToolTip and linkGroup,
//        // but this separation allows to manage the order on which elements are drew
//        // and so tooltips are always on top.
//        nodeGroup = svgGroup.append('g')
//                .attr('id', 'nodes');
//        linkGroup = svgGroup.append('g')
//                .attr('id', 'links');
//        linkGroupToolTip = svgGroup.append('g')
//                .attr('id', 'linksTooltips');
//        nodeGroupTooltip = svgGroup.append('g')
//                .attr('id', 'nodesTooltips');
//
//        defs = baseSvg.append('defs');
//        initArrowDef();
//        initDropShadow();
//
//        update(root);
//    }
//
//    function update(source) {
//        var nodes = tree.nodes(root).reverse(),
//                links = tree.links(nodes);
//
//        // Check if two nodes are in collision on the ordinates axe and move them
//        breadthFirstTraversal(tree.nodes(root), collision);
//        // Normalize for fixed-depth
//        nodes.forEach(function (d) {
//            d.y = d.depth * (rectNode.width * 1.5);
//        });
//
//        // 1) ******************* Update the nodes *******************
//        var node = nodeGroup.selectAll('g.node').data(nodes, function (d) {
//            return d.id || (d.id = ++i);
//        });
//        var nodesTooltip = nodeGroupTooltip.selectAll('g').data(nodes, function (d) {
//            return d.id || (d.id = ++i);
//        });
//
//        // Enter any new nodes at the parent's previous position
//        // We use "insert" rather than "append", so when a new child node is added (after a click)
//        // it is added at the top of the group, so it is drawed first
//        // else the nodes tooltips are drawed before their children nodes and they
//        // hide them
//        var nodeEnter = node.enter().insert('g', 'g.node')
//                .attr('class', 'node')
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y0 + ',' + source.x0 + ')';
//                })
//                .on('click', function (d) {
//                    click(d);
//                });
//        var nodeEnterTooltip = nodesTooltip.enter().append('g')
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y0 + ',' + source.x0 + ')';
//                });
//
//        nodeEnter.append('g').append('rect')
//                .attr('rx', 6)
//                .attr('ry', 6)
//                .attr('width', rectNode.width)
//                .attr('height', rectNode.height)
//                .attr('class', 'node-rect')
//                .attr('nodeDepth', function (d) {
//
//                    if (d["depth"] == 0)
//                        return 'nodeDepthLevel0';
//                    if (d["depth"] == 1)
//                        return 'nodeDepthLevel1';
//                    if (d["depth"] == 2)
//                        return 'nodeDepthLevel2';
//                    if (d["depth"] == 3)
//                        return 'nodeDepthLevel3';
//                    if (d["depth"] == 4)
//                        return 'nodeDepthLevel4';
//                })
//                .attr('fill', function (d) {
//                    if (d["depth"] == 0)
//                        return $("#colorAdderlevel0Secondary").val();
//                    if (d["depth"] == 1)
//                        return $("#colorAdderlevel1Secondary").val();
//                    if (d["depth"] == 2)
//                        return $("#colorAdderlevel2Secondary").val();
//                    if (d["depth"] == 3)
//                        return $("#colorAdderlevel3Secondary").val();
//                    if (d["depth"] == 4)
//                        return $("#colorAdderlevel4Secondary").val();
//                })
//                .attr('filter', 'url(#drop-shadow)');
//
//        nodeEnter.append('foreignObject')
//                .attr('x', rectNode.textMargin)
//                .attr('y', rectNode.textMargin)
//                .attr('width', function () {
//                    return (rectNode.width - rectNode.textMargin * 2) < 0 ? 0
//                            : (rectNode.width - rectNode.textMargin * 2)
//                })
//                .attr('height', function () {
//                    return (rectNode.height - rectNode.textMargin * 2) < 0 ? 0
//                            : (rectNode.height - rectNode.textMargin * 2)
//                })
//                .append('xhtml').html(function (d) {
//            return '<div style="width: '
//                    + (rectNode.width - rectNode.textMargin * 2) + 'px; height: '
//                    + (rectNode.height - rectNode.textMargin * 2) + 'px;" class="node-text wordwrap">'
//                    + '<b>' + d.nodeName + '</b><br><br>'
////                    + '<b>Plant: </b>' + d.plant + '<br>'
////                    + '<b>Instance: </b>' + d.instance + '<br>'
//                    + '</div>';
//        })
//                .on('mouseover', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
//                })
//                .on('mouseout', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
//                });
//
//        nodeEnterTooltip.append("rect")
//                .attr('id', function (d) {
//                    return 'nodeInfoID' + d.id;
//                })
//                .attr('x', rectNode.width / 2)
//                .attr('y', rectNode.height / 2)
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .attr('class', 'tooltip-box')
//                .style('fill-opacity', 0.8)
//                .on('mouseover', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'visible');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
//                    removeMouseEvents();
//                })
//                .on('mouseout', function (d) {
//                    $('#nodeInfoID' + d.id).css('visibility', 'hidden');
//                    $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
//                    reactivateMouseEvents();
//                });
//
//        nodeEnterTooltip.append("text")
//                .attr('id', function (d) {
//                    return 'nodeInfoTextID' + d.id;
//                })
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('y', rectNode.height / 2 + tooltip.textMargin * 2)
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .attr('class', 'tooltip-text')
//                .style('fill', 'white')
//                .append("tspan")
//                .text(function (d) {
//                    return 'Name: ' + d.name;
//                })
//                .append("tspan")
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return 'Supply:  DOMESTIC & NON-DOMESTIC';
//                })
//                .append("tspan")
//                .attr('x', rectNode.width / 2 + tooltip.textMargin)
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return 'Demand: 18';
//                });
//
//        // Transition nodes to their new position.
//        var nodeUpdate = node.transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + d.y + ',' + d.x + ')';
//                });
//        nodesTooltip.transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + d.y + ',' + d.x + ')';
//                });
//
//        nodeUpdate.select('rect')
//                .attr('class', function (d) {
//                    return d._children ? 'node-rect-closed' : 'node-rect';
//                });
//
//        nodeUpdate.select('text').style('fill-opacity', 1);
//
//        // Transition exiting nodes to the parent's new position
//        var nodeExit = node.exit().transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y + ',' + source.x + ')';
//                })
//                .remove();
//        nodesTooltip.exit().transition().duration(duration)
//                .attr('transform', function (d) {
//                    return 'translate(' + source.y + ',' + source.x + ')';
//                })
//                .remove();
//
//        nodeExit.select('text').style('fill-opacity', 1e-6);
//
//
//        // 2) ******************* Update the links *******************
//        var link = linkGroup.selectAll('path').data(links, function (d) {
//            return d.target.id;
//        });
//        var linkTooltip = linkGroupToolTip.selectAll('g').data(links, function (d) {
//            return d.target.id;
//        });
//
//        function linkMarkerStart(direction, isSelected) {
//            if (direction == 'SYNC')
//            {
//                return isSelected ? 'url(#start-arrow-selected)' : 'url(#start-arrow)';
//            }
//            return '';
//        }
//
//        function linkType(link) {
//            if (link.direction == 'SYNC')
//                return "Synchronous [\u2194]";
//            else
//            {
//                if (link.direction == 'ASYN')
//                    return "Asynchronous [\u2192]";
//            }
//            return '???';
//        }
//
//        d3.selection.prototype.moveToFront = function () {
//            return this.each(function () {
//                this.parentNode.appendChild(this);
//            });
//        };
//
//        // Enter any new links at the parent's previous position.
//        // Enter any new links at the parent's previous position.
//        var linkenter = link.enter().insert('path', 'g')
//                .attr('class', 'link')
//                .attr('id', function (d) {
//                    return 'linkID' + d.target.id;
//                })
//                .attr('d', function (d) {
//                    return diagonal(d);
//                })
//                .attr('marker-end', 'url(#end-arrow)')
//                .attr('marker-start', function (d) {
//                    return linkMarkerStart(d.target.link.direction, false);
//                })
//                .on('mouseover', function (d) {
//                    d3.select(this).moveToFront();
//
//                    d3.select(this).attr('marker-end', 'url(#end-arrow-selected)');
//                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, true));
//                    d3.select(this).attr('class', 'linkselected');
//
//                    $('#tooltipLinkID' + d.target.id).attr('x', (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y);
//                    $('#tooltipLinkID' + d.target.id).attr('y', (d.target.x - d.source.x) / 2 + d.source.x);
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
//                })
//                .on('mouseout', function (d) {
//                    d3.select(this).attr('marker-end', 'url(#end-arrow)');
//                    d3.select(this).attr('marker-start', linkMarkerStart(d.target.link.direction, false));
//                    d3.select(this).attr('class', 'link');
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
//                });
//
//        linkTooltip.enter().append('rect')
//                .attr('id', function (d) {
//                    return 'tooltipLinkID' + d.target.id;
//                })
//                .attr('class', 'tooltip-box')
//                .style('fill-opacity', 0.8)
//                .attr('x', function (d) {
//                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y;
//                })
//                .attr('y', function (d) {
//                    return (d.target.x - d.source.x) / 2 + d.source.x;
//                })
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .on('mouseover', function (d) {
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
//                    // After selected a link, the cursor can be hover the tooltip, that's why we still need to highlight the link and the arrow
//                    $('#linkID' + d.target.id).attr('class', 'linkselected');
//                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow-selected)');
//                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, true));
//
//                    removeMouseEvents();
//                })
//                .on('mouseout', function (d) {
//                    $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
//                    $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
//                    $('#linkID' + d.target.id).attr('class', 'link');
//                    $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow)');
//                    $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, false));
//
//                    reactivateMouseEvents();
//                });
//
//        linkTooltip.enter().append('text')
//                .attr('id', function (d) {
//                    return 'tooltipLinkTextID' + d.target.id;
//                })
//                .attr('class', 'tooltip-text')
//                .attr('x', function (d) {
//                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
//                })
//                .attr('y', function (d) {
//                    return (d.target.x - d.source.x) / 2 + d.source.x + tooltip.textMargin * 2;
//                })
//                .attr('width', tooltip.width)
//                .attr('height', tooltip.height)
//                .style('fill', 'white')
//                .append("tspan")
//                .text(function (d) {
//                    return linkType(d.target.link);
//                })
//                .append("tspan")
//                .attr('x', function (d) {
//                    return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin;
//                })
//                .attr('dy', '1.5em')
//                .text(function (d) {
//                    return d.target.link.name;
//                });
//
//        // Transition links to their new position.
//        var linkUpdate = link.transition().duration(duration)
//                .attr('d', function (d) {
//                    return diagonal(d);
//                });
//        linkTooltip.transition().duration(duration)
//                .attr('d', function (d) {
//                    return diagonal(d);
//                });
//
//        // Transition exiting nodes to the parent's new position.
//        link.exit().transition()
//                .remove();
//
//        linkTooltip.exit().transition()
//                .remove();
//
//        // Stash the old positions for transition.
//        nodes.forEach(function (d) {
//            d.x0 = d.x;
//            d.y0 = d.y;
//        });
//    }
//
//    // Zoom functionnality is desactivated (user can use browser Ctrl + mouse wheel shortcut)
//    function zoomAndDrag() {
//        //var scale = d3.event.scale,
//        var scale = 1,
//                translation = d3.event.translate,
//                tbound = -height * scale,
//                bbound = height * scale,
//                lbound = (-width + margin.right) * scale,
//                rbound = (width - margin.left) * scale;
//        // limit translation to thresholds
//        translation = [
//            Math.max(Math.min(translation[0], rbound), lbound),
//            Math.max(Math.min(translation[1], bbound), tbound)
//        ];
//        d3.select('.drawarea')
//                .attr('transform', 'translate(' + translation + ')' +
//                        ' scale(' + scale + ')');
//    }
//
//    // Toggle children on click.
//function click(d) {
//        if (d['children'] || d['_children']) {
//            if (d.children) {
//                d._children = d.children;
//                d.children = null;
//            } else {
//                d.children = d._children;
//                d._children = null;
//                var nodeDepth = d.depth;
//                    if (root['prevDepth'+nodeDepth] && root['prevDepth'+nodeDepth] != undefined && root['prevDepth'+nodeDepth] != null) {
//                        if (d.id != root['prevDepth'+nodeDepth]['id'])
//                            collapseDepth(root['prevDepth'+nodeDepth], nodeDepth);
//                    }
//                    root['prevDepth'+nodeDepth] = d;
//
//            }
//            update(d);
//        }
//    }
//    //17522
//    function collapseDepth(d, x) {
//        if (d.children) {
//            d._children = d.children;
//            d.children.forEach(function (d) {
//                if (d.children) {
//                    d._children = d.children;
//                    d.children = null;
//                }
//            });
//            d.children = null;
//        }
//    }
//
//    // Breadth-first traversal of the tree
//    // func function is processed on every node of a same level
//    // return the max level
//    function breadthFirstTraversal(tree, func)
//    {
//        var max = 0;
//        if (tree && tree.length > 0)
//        {
//            var currentDepth = tree[0].depth;
//            var fifo = [];
//            var currentLevel = [];
//
//            fifo.push(tree[0]);
//            while (fifo.length > 0) {
//                var node = fifo.shift();
//                if (node.depth > currentDepth) {
//                    func(currentLevel);
//                    currentDepth++;
//                    max = Math.max(max, currentLevel.length);
//                    currentLevel = [];
//                }
//                currentLevel.push(node);
//                if (node.children) {
//                    for (var j = 0; j < node.children.length; j++) {
//                        fifo.push(node.children[j]);
//                    }
//                }
//            }
//            func(currentLevel);
//            return Math.max(max, currentLevel.length);
//        }
//        return 0;
//    }
//
//    // x = ordoninates and y = abscissas
//    function collision(siblings) {
//        var minPadding = 5;
//        if (siblings) {
//            for (var i = 0; i < siblings.length - 1; i++)
//            {
//                if (siblings[i + 1].x - (siblings[i].x + rectNode.height) < minPadding)
//                    siblings[i + 1].x = siblings[i].x + rectNode.height + minPadding;
//            }
//        }
//    }
//
//    function removeMouseEvents() {
//        // Drag and zoom behaviors are temporarily disabled, so tooltip text can be selected
//        mousedown = d3.select('#dxpDecomposeTreeClass').select('svg').on('mousedown.zoom');
//        d3.select('#dxpDecomposeTreeClass').select('svg').on("mousedown.zoom", null);
//    }
//
//    function reactivateMouseEvents() {
//        // Reactivate the drag and zoom behaviors
//        d3.select('#dxpDecomposeTreeClass').select('svg').on('mousedown.zoom', mousedown);
//    }
//
//    // Name of the event depends of the browser
//    function getMouseWheelEvent() {
//        if (d3.select('#dxpDecomposeTreeClass').select('svg').on('wheel.zoom'))
//        {
//            mouseWheelName = 'wheel.zoom';
//            return d3.select('#dxpDecomposeTreeClass').select('svg').on('wheel.zoom');
//        }
//        if (d3.select('#dxpDecomposeTreeClass').select('svg').on('mousewheel.zoom') != null)
//        {
//            mouseWheelName = 'mousewheel.zoom';
//            return d3.select('#dxpDecomposeTreeClass').select('svg').on('mousewheel.zoom');
//        }
//        if (d3.select('#dxpDecomposeTreeClass').select('svg').on('DOMMouseScroll.zoom'))
//        {
//            mouseWheelName = 'DOMMouseScroll.zoom';
//            return d3.select('#dxpDecomposeTreeClass').select('svg').on('DOMMouseScroll.zoom');
//        }
//    }
//
//    function diagonal(d) {
//        var p0 = {
//            x: d.source.x + rectNode.height / 2,
//            y: (d.source.y + rectNode.width)
//        }, p3 = {
//            x: d.target.x + rectNode.height / 2,
//            y: d.target.y - 12 // -12, so the end arrows are just before the rect node
//        }, m = (p0.y + p3.y) / 2, p = [p0, {
//                x: p0.x,
//                y: m
//            }, {
//                x: p3.x,
//                y: m
//            }, p3];
//        p = p.map(function (d) {
//            return [d.y, d.x];
//        });
//        return 'M' + p[0] + 'C' + p[1] + ' ' + p[2] + ' ' + p[3];
//    }
//
//    function initDropShadow() {
//        var filter = defs.append("filter")
//                .attr("id", "drop-shadow")
//                .attr("color-interpolation-filters", "sRGB");
//
//        filter.append("feOffset")
//                .attr("result", "offOut")
//                .attr("in", "SourceGraphic")
//                .attr("dx", 0)
//                .attr("dy", 0);
//
//        filter.append("feGaussianBlur")
//                .attr("stdDeviation", 2);
//
//        filter.append("feOffset")
//                .attr("dx", 2)
//                .attr("dy", 2)
//                .attr("result", "shadow");
//
//        filter.append("feComposite")
//                .attr("in", 'offOut')
//                .attr("in2", 'shadow')
//                .attr("operator", "over");
//    }
//
//    function initArrowDef() {
//        // Build the arrows definitions
//        // End arrow
//        defs.append('marker')
//                .attr('id', 'end-arrow')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrow')
//                .append('path')
//                .attr('d', 'M0,-5L10,0L0,5');
//
//        // End arrow selected
//        defs.append('marker')
//                .attr('id', 'end-arrow-selected')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrowselected')
//                .append('path')
//                .attr('d', 'M0,-5L10,0L0,5');
//
//        // Start arrow
//        defs.append('marker')
//                .attr('id', 'start-arrow')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrow')
//                .append('path')
//                .attr('d', 'M10,-5L0,0L10,5');
//
//        // Start arrow selected
//        defs.append('marker')
//                .attr('id', 'start-arrow-selected')
//                .attr('viewBox', '0 -5 10 10')
//                .attr('refX', 0)
//                .attr('refY', 0)
//                .attr('markerWidth', 6)
//                .attr('markerHeight', 6)
//                .attr('orient', 'auto')
//                .attr('class', 'arrowselected')
//                .append('path')
//                .attr('d', 'M10,-5L0,0L10,5');
//    }
//    function changeColor(nodeColor, jsonData) {
//
//        var color = nodeColor;
//        tree = d3.layout.tree().size([height, width]);
//        var maxTreeWidth = breadthFirstTraversal(tree.nodes(jsonData), function (currentLevel) {
//
//            currentLevel.forEach(function (node) {
//                if (depthSelector == undefined || depthSelector == null)//25422
//                    node.color = '#AE7A9B';
//                if ((depthSelector == node['depth']) && depthSelector == 1)
//                    node.color = nodeColor;
//                if ((depthSelector == node['depth']) && depthSelector == 2)
//                    node.color = nodeColor;
//                if ((depthSelector == node['depth']) && depthSelector == 3)
//                    node.color = nodeColor;
//            });
//        });
//    }
//
//    function deepCollapse(d) {
//        d['children'].forEach((childObj) => {
//            if (childObj.children) {
//                childObj['children'].forEach(collapse);
//                if (childObj.hasOwnProperty('children') &&
//                        childObj.children instanceof Array &&
//                        childObj.children.length > 0) {
//                    childObj._children = childObj.children;
//                    childObj.children = null;
//                } else {
//                    console.log("no Child");
//                }
//            }
//        });
//    }
//}