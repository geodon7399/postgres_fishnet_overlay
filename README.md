<h1><b>Percent Overlap of Census Blocks - PostGIS/Fishnet Overlay</b></h1> 

<b>The Goal</b>
--------------------------
Determine percent overlap of telecom carrier coverage with 2010 census blocks at a nationwide level. 


<b>The Problem</b>
--------------------------
There are approximately 20 shape files representing each carriers' coverage footprint. Each of them consist of numerous multipolygons with many of them being complex (containing 10000+ vertices) polygons which can be too process intensive for the resources when performing spatial operations on the data.


<b>The Solution</b>
--------------------------
Utilize Postgres/PostGIS to dice complex polygons and perform spatial overlays and using Python to automate the process.


<b>The Tools/Data Configuration</b>
--------------------------
* Postgres 9.2/PostGIS 2.0
    - loaded 2010 census block data into database
    - created a parent block table and children block tables by state. This was done to increase performance by only pulling out census blocks needed for spatial operations.
    - load all carrier shape files using shp2psql function
    - create "fishnet" function in Postgres. This function is used to dice large polygons into smaller chunks.
* Python 2.7.3
* Psycop2 2.4.3
    - external library to send SQL statements to Postgres


<b>What The Code Does</b>
--------------------------
* reads all coverages in the schema
* uses st_npoints to determine if coverage polygons are large.
* send large polygons to the fishnet function to dice them into smaller chunks
* inserts the chunks back into the coverage table and deletes the complex polygon
* uses st_contains to determine 100% overlap
* uses st_intersect to determine any interaction with census blocks
* take the difference between the result of st_contains and st_intersects and perform st_intersection to get partially covered blocks
* create and write results to a temp table
* create final output table from temp table and group/sum percent overlap. This is done since there are multiple polygons overlapping the same census block
* creates a log file of the processing status and elapsed time of certain parts and the entire process
* repeats process for each coverage table


<b>What's Next</b>
--------------------------
* Modularize parts of the script. The fishnet, dicing, SQL statements sent to Postgres can be separated into their own modules to improve readability of the script.
* Improve how multiple coverage polygons in partially covered census blocks are calculated. Current method is not entirely accurate but is close.

