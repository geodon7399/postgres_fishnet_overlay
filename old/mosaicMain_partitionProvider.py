import psycopg2, donPG_Tools

t = donPG_Tools
conn = psycopg2.connect("host=localhost port=5432 dbname=donGIS user=postgres password=nikki7399")
cur = conn.cursor()

#CREATE STATE LIST
#states = ["01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19","20","21",
#                "22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39",
#                "40","41","42","44","45","46","47","48","49","50","51","53","54","55","56","60","66","69","72","78"]

states = ["11"]

#SELECT PROVIDER TABLES IN SWAT SCHEMA
providerTables = """select table_name from information_schema.tables
where table_schema = 'swat'
and table_name in ('att_umts')
order by table_name;"""

#PUT PROVIDER TABLE NAMES INTO A LIST
cur.execute(providerTables)
providerTables = cur.fetchall()

for provider in providerTables:
    #try:
    # CREATE A CSV FILE BY PROVIDER NAME TO SAVE DATA TO

    provider = provider[0]
    f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'w')
    f.writelines('mkt_name,entity,protocol,geoid10,pct_overlap\n')
    
    # SELECT DISTINCT MKT_NAME, ENTITIY, PROTOCOL FROM PROVIDER TABLE
    providerRec = """select distinct mkg_name, entity, protocol from swat.%s""" % (provider)
    cur.execute(providerRec)
    providerRec = cur.fetchall()
    
    # CLEAN UP PROVIDER RECORD TO BE SAVED TO THE CSV FILE
    providerRec = ([tup[:3] for tup in providerRec])
    providerRec = str(providerRec[0])
    providerRec = providerRec.strip('('')')
    providerRec = providerRec.replace("'","")
    #print providerRec
    
    #create a dynamic list of states
    sProviderRecs = """select s.gid, s.mkg_name, s.entity, s.protocol, b.state_fips 
                        from swat.att_umts s, base.state b
                            where st_intersects(s.geom, b.geom)
                            group by s.gid, s.mkg_name, s.entity, s.protocol, b.state_fips
                            order by s.gid;"""
    
    cur.execute(sProviderRecs)
    sProviderRecs = cur.fetchall()
    
    for rec in sProviderRecs:
        providerGeom = """select ST_AsText(geom) from swat.att_umts where gid = %s order by gid;""" % (rec[0])
        cur.execute(providerGeom)
        providerGeom = cur.fetchall()
        providerGeom = str(providerGeom[0])
        providerGeom = providerGeom.strip('('')')
        providerGeom = providerGeom[:-1]
        #print providerGeom

#        for state in states:
#            
#            # SELECT BLOCKS THAT ARE COVERED BY THE PROVIDER 100%
#            block100 = """select b.geoid10 
#                            from swat.att_umts s, cbpoly.block b
#                            where st_contains(%s, b.geom) 
#                            and b.statefp10 = '%s'""" % (geom,state)
#            print block100
#            
#            # SELECT BLOCKS THAT ARE < 100% COVERED BY THE PROVIDER
#            blockIntersect = """select b.geoid10 
#                                from swat.att_umts s, cbpoly.block b
#                                where st_intersects(b.geom, %s) 
#                                and b.statefp10 = '%s'""" % (geom,state)
#    
#            cur.execute(block100)
#            block100 = cur.fetchall()
#            block100b = []
#            for block in block100:
#                block = block[0]
#                block100b.append(block)
#                f.writelines(providerRec + ', ' + block + ', ' + '1\n')
#                #print providerRec + ', ' + block + ', ' + '1'
#    
#            cur.execute(blockIntersect)
#            blockIntersect = cur.fetchall()
#            blockIntersectb = []
#            for block in blockIntersect:
#                block = block[0]
#                blockIntersectb.append(block)
#            
#            
#            # GET BLOCKS THAT ONLY OVERLAP
#            overlap = list(set(blockIntersectb) - set(block100b))
#            print str(len(overlap))
#            
#    #        overlap = [i for i, j in zip(block100b,blockIntersectb) if i!=j]
#    #        print str(len(overlap))         
        
        
        
        
        
        
        
        #f.close()
    #except:
        #print "error"
            
            
            