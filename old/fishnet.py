import psycopg2

conn = psycopg2.connect("host=localhost port=5432 dbname=donGIS user=postgres password=nikki7399")
cur = conn.cursor()

#SELECT PROVIDER TABLES IN SWAT SCHEMA
providerTables = """select table_name from information_schema.tables
where table_schema = 'swat'
and table_name in ('test')
order by table_name;"""

#PUT PROVIDER TABLE NAMES INTO A LIST
cur.execute(providerTables)
providerTables = cur.fetchall()

for provider in providerTables:

    #create a dynamic list of states
    sProviderRecs = """select *, st_npoints(geom) from swat.%s
                        order by st_npoints desc, gid;""" % (provider)
    
    cur.execute(sProviderRecs)
    sProviderRecs = cur.fetchall()
    
    for rec in sProviderRecs:
        #print rec
        
        nPoints = rec[5]
        if nPoints > 10000:
            print rec
