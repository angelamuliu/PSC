
// For changing active dimensions (default is filereads/filewrites)
var dimensions = ["filereads", "filewrites", "CPU", "diskIOBW", "net bytes"];
var xdim_index = 0; var ydim_index = 1;
var xdim = dimensions[xdim_index]; var ydim = dimensions[ydim_index];

function changeView() {
    xdim_index = $("#xSelect").val(); ydim_index = $("#ySelect").val();
    xdim = dimensions[xdim_index]; ydim = dimensions[ydim_index];
    $( "div#kmeans-vis" ).remove();
    var e = $('<div id="kmeans-vis"></div>');
    $(e).appendTo("body");
    makeSVG();
}

// Generates the SVG based on active dimensions
function makeSVG(){
    // Generating list of colors, setting width/heigh, etc
    colors = d3.scale.category20();
    var width = 600;
    var height = 450;
    var padding = 40;
    
    svg = d3.select("#kmeans-vis").append("svg")
        .attr("width", width+2*padding)
        .attr("height", height+2*padding);
    
    // HELPER: Given a value, built a string that can be used to render a SVG path
    var buildPathFromPoint = function(point) {
        return "M" + point.join("L") + "Z";
    }
    // HELPER: To find a fill color
    var findcolor = function(d,ndx) {
        return colors(ndx);
    }
    
    // ---------------------------------------------
    
    d3.text("kmeansCSV.csv", function(unparsedData){
        parsedData = d3.csv.parse(unparsedData);
        // Creating arrays of number values per dimension
        xset = []; yset = [];
        for(var i=0, len=parsedData.length; i<len; i++) {
            xset.push(+parsedData[i][xdim]);
            yset.push(+parsedData[i][ydim]);
        }
        
        // d3 scale used to scale original data (domain) into new range
        xScale = d3.scale.linear()
                    .domain([d3.min(xset), d3.max(xset)])
                    .range([0,width]);
        yScale = d3.scale.linear()
                    .domain([d3.min(yset), d3.max(yset)])
                    .range([height,0]);
        
        // Loading centriods
        d3.text("kmeansCentersCSV.csv", function(unparsedCenters) {
            parsedCenters = d3.csv.parse(unparsedCenters);
            centroids = []
            for(var i=0, len=parsedCenters.length; i<len; i++) {
                centroids.push([+parsedCenters[i][xdim], +parsedCenters[i][ydim]]);
            }
            // VORONOI -------------------------------
            // Let voronoi know what you want to use as x and y for centers
            voronoi = d3.geom.voronoi()
                .x(function(d) { return xScale(d[0]); })
                .y(function(d) { return yScale(d[1]); });
            voronoiGroup   = svg.append('g').attr('id', 'voronoi');
            voronoiGroup.selectAll('*').remove();
            
            // Rendering the voronoi paths
            voronoiGroup.selectAll('path')
                .data(voronoi(centroids))
                .enter().append('path')
                .style("fill", findcolor)
                .attr("d", buildPathFromPoint) //...next line is adding padding
                .attr("transform", "translate("+padding+","+padding+")");
            // ---------------------------------------
            
            
            // Creating d3 scaled axis ---------------
            var xAxis = d3.svg.axis().scale(xScale).orient("bottom"); // orient determines location of tick labels
            var yAxis = d3.svg.axis().scale(yScale).orient("left");
            
            var xAxisGroup = svg.append("g")
                .attr("class", "axis")
                .attr("transform", "translate("+padding+"," + (height+padding) + ")") //bottom allign
                .call(xAxis)// calls the xAxis object to render
                    .selectAll("text")
                    .attr("transform", "rotate(15)")
                    .style("text-anchor", "start"); // rotating x axis labels
            var yAxisGroup = svg.append("g")
                .attr("class", "axis")
                .attr("transform", "translate("+padding+","+padding+")") //left allign
                .call(yAxis);
            // ---------------------------------------
            
            
            // Plotting the actual data points
            for(var i=0, len=parsedData.length; i<len; i++) {
                svg.append("circle")
                .attr("cx", xScale(xset[i]))
                .attr("cy", yScale(yset[i]))
                .attr("r", 2)
                .attr("transform", "translate("+padding+","+padding+")");
            }
            // ---------------------------------------
            
            
            // Plotting centroids --------------------
            centroidsGroup = svg.append('g').attr('id', 'centroids')
            centroidsGroup.selectAll('*').remove()
            centroidsGroup.selectAll('circle')
                .data(centroids).enter()
                .append('circle')
                .attr("cx", function(d){ return xScale(d[0])})
                .attr("cy", function(d){ return yScale(d[1])})
                .attr("r", 2)
                .style("fill", "white")
                .attr("transform", "translate("+padding+","+padding+")");
            // ---------------------------------------
        
        }); // close centriods csv loop
    }); // close loading csv
}; // close makeSVG()

$(document).ready(makeSVG);

$(function() {
    $( ".datepicker" ).datepicker();
});