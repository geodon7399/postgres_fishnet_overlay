<h1><b>Percent Overlap of Census Blocks - PostGIS/Fishnet Overlay</b></h1> 

<b>The Goal</b>
--------------------------
Determine percent overlap of telecom carrier coverage with 2010 census blocks at a nationwide level. 


<b>The Problem</b>
--------------------------
There are approximately 20 shape files representing each carriers' coverage footprint per technology. Each of them consist of numerous multipolygons with many of them being complex (containing 10000+ vertices) polygons which heavily impact computer resources when performing spatial operations on the data.


<b>The Solution</b>
--------------------------
Utilize Postgres/PostGIS to dice complex polygons and perform spatial overlays and using Python to automate the process.


<b>The Tools/Data Configuration</b>
--------------------------
* Postgres 9.2/PostGIS 2.0
    - loaded 2010 census block data into database
    - created a parent block table
    - created children block tables with check constraints by state. This was done to increase performance by only selecting census blocks needed for spatial operations.
    - load all carrier shape files into database
    - create "fishnet" function in Postgres. I grabbed this function from a thread on StackOverflow. This function is used to dice large polygons into smaller chunks
    - create a test_grid table to hold the results of st_intersection with coverage and fishnet
* Python 2.7.4
* Psycopg2 2.4.6
    - external library to send SQL statements to Postgres


<b>What The Code Does</b>
--------------------------
* reads all coverages in the schema into the process
* repairs self-intersecting geometries using st_buffer and converts geometries to multipolygon using st_multi
* uses st_npoints to determine if coverage polygons are large. If so...
    * sends large polygons to the fishnet function to dice them into smaller chunks
    * inserts the chunks back into the coverage table and deletes the complex polygon
* uses st_contains to determine 100% overlap and writes results immediately to the temp table. This was used to save processing time since st_contains already determines the blocks are covered 100%
* uses st_intersect to determine any interaction with coverage/census blocks per state
* the results of st_contains and st_intersects are put into python lists. Then we take the difference between the result and get a list of blocks that are partially covered
* runs st_intersection on partially covered blocks ans st_area to determine percent overlap
* creates and write results to a temp table
* creates final output table by aggregating data from the temp table and summing the percent overlap. This is done since there may be multiple polygons overlapping the same census block
* creates a log file of the processing status and elapsed time of certain parts of the process and how long it takes to process each coverage
* repeats process for each coverage table


<b>What's Next</b>
--------------------------
* modularize the script. The fishnet, dicing, SQL statements sent to Postgres, etc can be separated into their own modules to improve readability of the process
* parameters for the fishnet function are hard-coded. Need to set parameters dynamically specific to each coverage
* improve how multiple coverage polygons in partially covered census blocks are calculated. Current method is not   entirely accurate (off by 1% or less) as more than one coverage polygon may be covering parts of the same area in a block. This is a rare occurance though and seems to be low risk
* figure out a way to process the data in memory instead of creating temp tables and the test_grid table
* investigate ArcGIS's Dice toolbox. A vertex limit can be set on the data and the tool dices the data x times. The tool seemed faster than dicing in postgres
