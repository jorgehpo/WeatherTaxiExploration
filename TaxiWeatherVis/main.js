"use strict";
$( document ).ready(initMap);

var map;
var group = {};
var heatmap;
var heatmapData = new google.maps.MVCArray();

var dimension = {};
var data, parameters;


//--------------------------

var map2;
var group2 = {};
var heatmap2;
var heatmapData2 = new google.maps.MVCArray()
var dimension2 = {};
var data2;

//--------------------------

var mouseOverMap = 0;

function createFilterFunction(input){
    var query = function(x){
        var selected = input.val();
        if (selected == null){
            return true;
        }else{
            for (var i = 0; i < selected.length; i++){
                if (x == selected[i])
                    return true;
            }
            return false;
        }
    }
    return query;
}


function initMap() {
    $(".selectpicker").selectpicker();

    $("#rangeRadius").slider({
        formatter: function(value) {
            return 'Current value: ' + value;
        }
    });
    $("#rangeRadius2").slider({
        formatter: function(value) {
            return 'Current value: ' + value;
        }
    });

    map = new google.maps.Map(document.getElementById("map"), {
        center: {lat: 40.7128, lng: -74.0059},
        zoom: 12,
        mapTypeControlOptions: {
          mapTypeIds: [],
          disableDefaultUI: true
        },
        styles:[{"featureType":"administrative","elementType":"labels.text.fill","stylers":[{"color":"#444444"}]},{"featureType":"landscape","elementType":"all","stylers":[{"color":"#f2f2f2"}]},{"featureType":"poi","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"road","elementType":"all","stylers":[{"saturation":-100},{"lightness":45}]},{"featureType":"road.highway","elementType":"all","stylers":[{"visibility":"simplified"}]},{"featureType":"road.arterial","elementType":"labels.icon","stylers":[{"visibility":"off"}]},{"featureType":"transit","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"water","elementType":"all","stylers":[{"color":"#3182bd"},{"visibility":"on"}]}]
    });


    map2 = new google.maps.Map(document.getElementById("map2"), {
        center: {lat: 40.7128, lng: -74.0059},
        zoom: 12,
        mapTypeControlOptions: {
          mapTypeIds: [],
          disableDefaultUI: true
        },
        disableDoubleClickZoom: true,
        draggable: false,
        scrollwheel: false,
        panControl: false,

        styles:[{"featureType":"administrative","elementType":"labels.text.fill","stylers":[{"color":"#444444"}]},{"featureType":"landscape","elementType":"all","stylers":[{"color":"#f2f2f2"}]},{"featureType":"poi","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"road","elementType":"all","stylers":[{"saturation":-100},{"lightness":45}]},{"featureType":"road.highway","elementType":"all","stylers":[{"visibility":"simplified"}]},{"featureType":"road.arterial","elementType":"labels.icon","stylers":[{"visibility":"off"}]},{"featureType":"transit","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"water","elementType":"all","stylers":[{"color":"#3182bd"},{"visibility":"on"}]}]
    });

    google.maps.event.addListener(map, 'zoom_changed', function () {
          map2.setZoom(map.getZoom());
    });

    map.addListener('center_changed', function() {
        map2.panTo(map.getCenter());
    });





    heatmap = new google.maps.visualization.HeatmapLayer({
        data: heatmapData,
        radius:14
    });
    heatmap.setMap(map);


    heatmap2 = new google.maps.visualization.HeatmapLayer({
        data: heatmapData2,
        radius:14
    });
    heatmap2.setMap(map2);

    $("#rangeRadius").slider().on("slideStop",function(x){
        heatmap.setOptions({radius:+$("#rangeRadius").val()});
        heatmap2.setOptions({radius:+$("#rangeRadius").val()});
        $("#rangeRadius2").slider('setValue', +$("#rangeRadius").val());
    });

    $("#rangeRadius2").slider().on("slideStop",function(x){
        heatmap.setOptions({radius:+$("#rangeRadius2").val()});
        heatmap2.setOptions({radius:+$("#rangeRadius2").val()});
        $("#rangeRadius").slider('setValue', +$("#rangeRadius2").val());
    });

    $("#txtReport").text("0 taxi trips selected.");

    $("#queryBtn").click(function(){
        var total = 0
        dimension["typeOp"].filter( $('input[name=typeOp]:checked').val() );

        
        var start_latitude = parameters["min_latitude"] + parameters["size_bin_latitude"]/2;
        var start_longitude = parameters["min_longitude"] + parameters["size_bin_longitude"]/2;
        var filtered_data = dimension["typeOp"].top(Infinity);

        var nbins_latitude = parameters["nbins_latitude"];

        var nbins_longitude = parameters["nbins_longitude"];

        var size_bin_latitude = parameters["size_bin_latitude"];
        var size_bin_longitude = parameters["size_bin_longitude"];
        var heatmapAccumm = new Int32Array(nbins_latitude * nbins_longitude);

        heatmapData.clear();

        filtered_data.forEach(function(d){
            for (var i = 0; i < d["heatmap"].length; i++){
                var bin = d["heatmap"][i];
                var latitude =  bin[0];
                var longitude = bin[1];
                // latitude: i
                // longitude: j
                heatmapAccumm[latitude*nbins_longitude + longitude] += bin[2];
                total += bin[2];
            }
        });

        console.log(total)

        $("#txtReport").text(numeral(total).format("0,0") + " taxi trips selected.");

        for (var latitude = 0; latitude < nbins_latitude; latitude++){
            for (var longitude = 0; longitude < nbins_longitude; longitude++){
                if (heatmapAccumm[latitude*nbins_longitude + longitude] > 0){
                    heatmapData.push({location: new google.maps.LatLng(start_latitude + size_bin_latitude *latitude, start_longitude + size_bin_longitude * longitude), weight: heatmapAccumm[latitude*nbins_longitude + longitude]});
                }
            }                        
        }

        if (heatmapData.length > 0){
            heatmap.setMap(map);
        }else{
            heatmap.setMap();
        }

    } );


    $("#queryBtn2").click(function(){
        var total = 0
        dimension2["typeOp"].filter( $('input[name=typeOp]:checked').val() );

        
        var start_latitude = parameters["min_latitude"] + parameters["size_bin_latitude"]/2;
        var start_longitude = parameters["min_longitude"] + parameters["size_bin_longitude"]/2;
        var filtered_data = dimension2["typeOp"].top(Infinity);

        var nbins_latitude = parameters["nbins_latitude"];

        var nbins_longitude = parameters["nbins_longitude"];

        var size_bin_latitude = parameters["size_bin_latitude"];
        var size_bin_longitude = parameters["size_bin_longitude"];
        var heatmapAccumm = new Int32Array(nbins_latitude * nbins_longitude);

        heatmapData2.clear();

        filtered_data.forEach(function(d){
            for (var i = 0; i < d["heatmap"].length; i++){
                var bin = d["heatmap"][i];
                var latitude =  bin[0];
                var longitude = bin[1];
                // latitude: i
                // longitude: j
                heatmapAccumm[latitude*nbins_longitude + longitude] += bin[2];
                total += bin[2];
            }
        });

        console.log(total)

        $("#txtReport2").text(numeral(total).format("0,0") + " taxi trips selected.");

        for (var latitude = 0; latitude < nbins_latitude; latitude++){
            for (var longitude = 0; longitude < nbins_longitude; longitude++){
                if (heatmapAccumm[latitude*nbins_longitude + longitude] > 0){
                    heatmapData2.push({location: new google.maps.LatLng(start_latitude + size_bin_latitude *latitude, start_longitude + size_bin_longitude * longitude), weight: heatmapAccumm[latitude*nbins_longitude + longitude]});
                }
            }                        
        }

        if (heatmapData2.length > 0){
            heatmap2.setMap(map2);
        }else{
            heatmap2.setMap();
        }

    } );



    d3.json("taxi_weather_queries.json",function(data_json){

        parameters = data_json["parameters"];
        data = crossfilter(data_json["queries"]);
        data2 = crossfilter(data_json["queries"]);

        dimension["typeOp"] = data.dimension(function(fact){return fact["typeOp"]});
        dimension["weekday"] = data.dimension(function(fact){return fact["weekday"]});
        dimension["month"] = data.dimension(function(fact){return fact["month"]});
        dimension["hour"] = data.dimension(function(fact){return fact["hour"]});
        dimension["weather"] = data.dimension(function(fact){return fact["weather"]});
        dimension["temperature"] = data.dimension(function(fact){return fact["temperature"]});

        group["month"] = dimension["month"].group();
        group["weekday"] = dimension["weekday"].group();
        group["hour"] = dimension["hour"].group();
        group["temperature"] = dimension["temperature"].group();
        group["weather"] = dimension["weather"].group();
        group["typeOp"] = dimension["typeOp"].group();

        dimension2["typeOp"] = data2.dimension(function(fact){return fact["typeOp"]});
        dimension2["weekday"] = data2.dimension(function(fact){return fact["weekday"]});
        dimension2["month"] = data2.dimension(function(fact){return fact["month"]});
        dimension2["hour"] = data2.dimension(function(fact){return fact["hour"]});
        dimension2["weather"] = data2.dimension(function(fact){return fact["weather"]});
        dimension2["temperature"] = data2.dimension(function(fact){return fact["temperature"]});


        group2["month"] = dimension2["month"].group();
        group2["weekday"] = dimension2["weekday"].group();
        group2["hour"] = dimension2["hour"].group();
        group2["temperature"] = dimension2["temperature"].group();
        group2["weather"] = dimension2["weather"].group();
        group2["typeOp"] = dimension2["typeOp"].group();





        var monthChart = dc.rowChart('#monthChart');
        monthChart.width(180)
            .elasticX(true)
            .height(12*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["month"])
            .dimension(dimension["month"])
            .label(function (d){
                return ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"][d.key-1];
            });
        monthChart.colors(d3.scale.ordinal().range(['#3182bd']));
        monthChart.render();
                
        var dayOfWeekChart = dc.rowChart('#weekdayChart');
        dayOfWeekChart.width(180)
            .elasticX(true)
            .height(7*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["weekday"])
            .dimension(dimension["weekday"])
            .label(function (d){
                return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][d.key]
            }); 
        dayOfWeekChart.colors(d3.scale.ordinal().range(['#3182bd']));
        dayOfWeekChart.render();


        var hourChart = dc.rowChart('#hourChart');
        hourChart.width(180)
            .elasticX(true)
            .height(6*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["hour"])
            .dimension(dimension["hour"])
            .label(function (d){
                return ["0am - 4am","4am - 8am","8am - 12am","12pm - 4pm","4pm - 8pm","8pm - 12pm"][d.key]
            }); 
        hourChart.colors(d3.scale.ordinal().range(['#3182bd']));
        hourChart.render();


        var tempChart = dc.rowChart('#tempChart');
        tempChart.width(180)
            .elasticX(true)
            .height(5*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["temperature"])
            .dimension(dimension["temperature"])
            .label(function (d){
                return ["-20° F - 0° F","0° F - 20° F","20° F - 40° F","40° F - 60° F","60° F - 80° F","80° F - 100° F"][d.key]
            }); 
        tempChart.colors(d3.scale.ordinal().range(['#3182bd']));
        tempChart.render();


       var weatherChart = dc.rowChart('#weatherChart');
        weatherChart.width(180)
            .elasticX(true)
            .height(5*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["weather"])
            .dimension(dimension["weather"])
            .label(function (d){
                return {"NoPrecipitation":"No Precipitation", "Drizzle":"Drizzle", "Rain":"Rain", "SolidPrecipitation":"Solid Precipitation", "ShoweryPrecipitation":"Showery Precipitation"}[d.key]
            }); 
        weatherChart.colors(d3.scale.ordinal().range(['#3182bd']));
        weatherChart.render();



        //Drawing second plots. 

        var monthChart2 = dc.rowChart('#monthChart2');
        monthChart2.width(180)
            .elasticX(true)
            .height(12*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group2["month"])
            .dimension(dimension2["month"])
            .label(function (d){
                return ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"][d.key-1];
            });
        monthChart2.colors(d3.scale.ordinal().range(['#3182bd']));
        monthChart2.render();
                
        var dayOfWeekChart2 = dc.rowChart('#weekdayChart2');
        dayOfWeekChart2.width(180)
            .elasticX(true)
            .height(7*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group2["weekday"])
            .dimension(dimension2["weekday"])
            .label(function (d){
                return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][d.key]
            }); 
        dayOfWeekChart2.colors(d3.scale.ordinal().range(['#3182bd']));
        dayOfWeekChart2.render();


        var hourChart2 = dc.rowChart('#hourChart2');
        hourChart2.width(180)
            .elasticX(true)
            .height(6*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group2["hour"])
            .dimension(dimension2["hour"])
            .label(function (d){
                return ["0am - 4am","4am - 8am","8am - 12am","12pm - 4pm","4pm - 8pm","8pm - 12pm"][d.key]
            }); 
        hourChart2.colors(d3.scale.ordinal().range(['#3182bd']));
        hourChart2.render();


        var tempChart2 = dc.rowChart('#tempChart2');
        tempChart2.width(180)
            .elasticX(true)
            .height(5*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group2["temperature"])
            .dimension(dimension2["temperature"])
            .label(function (d){
                return ["-20° F - 0° F","0° F - 20° F","20° F - 40° F","40° F - 60° F","60° F - 80° F","80° F - 100° F"][d.key]
            }); 
        tempChart2.colors(d3.scale.ordinal().range(['#3182bd']));
        tempChart2.render();


       var weatherChart2 = dc.rowChart('#weatherChart2');
        weatherChart2.width(180)
            .elasticX(true)
            .height(5*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group2["weather"])
            .dimension(dimension2["weather"])
            .label(function (d){
                return {"NoPrecipitation":"No Precipitation", "Drizzle":"Drizzle", "Rain":"Rain", "SolidPrecipitation":"Solid Precipitation", "ShoweryPrecipitation":"Showery Precipitation"}[d.key]
            }); 
        weatherChart2.colors(d3.scale.ordinal().range(['#3182bd']));
        weatherChart2.render();

      });
}//initmap
