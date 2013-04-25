import psycopg2

conn = psycopg2.connect("host=localhost port=5432 dbname=donGIS user=postgres password=nikki7399")
cur = conn.cursor()

#SELECT PROVIDER TABLES IN SWAT SCHEMA
providerTables = """select table_name from information_schema.tables
where table_schema = 'swat'
and table_name in ('c_spire_wireless_lte')
order by table_name;"""

#PUT PROVIDER TABLE NAMES INTO A LIST
cur.execute(providerTables)
providerTables = cur.fetchall()

for provider in providerTables:
    #try:
    # CREATE A CSV FILE BY PROVIDER NAME TO SAVE DATA TO

    provider = provider[0]
    print provider
    f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'w')
    f.writelines('id,prov_id,mkt_name,entity,protocol,geoid10,pct_overlap\n')
    f.close()
    
    #create a dynamic list of states
    sProviderRecs = """select s.gid, s.mkg_name, s.entity, s.protocol, b.state_fips 
                        from swat.%s s, base.state b
                            where st_intersects(s.geom, b.geom)
                            and st_npoints(s.geom) < 40000
                            group by s.gid, s.mkg_name, s.entity, s.protocol, b.state_fips
                            order by s.gid;""" % (provider)
    
    cur.execute(sProviderRecs)
    sProviderRecs = cur.fetchall()
    
    #create unique identifier in the csv file
    id=0
    
    for rec in sProviderRecs:
        f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'a')
        #print 'processing gid '+ str(rec[0])
        print rec
            
        # SELECT BLOCKS THAT ARE COVERED BY THE PROVIDER 100%
        block100 = """select b.geoid10 
                        from swat.%s s, cbpoly.block b
                        where st_contains(s.geom, b.geom)
                        and s.gid = %s 
                        and b.statefp10 = '%s'""" % (provider,rec[0],rec[4])
        #print block100
            
        # SELECT BLOCKS THAT ARE < 100% COVERED BY THE PROVIDER
        blockIntersect = """select b.geoid10 
                            from swat.%s s, cbpoly.block b
                            where st_intersects(b.geom, s.geom)
                            and s.gid = %s
                            and b.statefp10 = '%s'""" % (provider,rec[0],rec[4])
    
        cur.execute(block100)
        block100 = cur.fetchall()
        block100b = []
        for block in block100:
            block = block[0]
            block100b.append(block)
            id=id+1
            f.writelines(str(id) + ', ' + str(rec[0]) + ', ' + rec[1] + ', ' + rec[2] + ', ' + rec[3] + ', ' + block + ', ' + '1\n')
            #print providerRec + ', ' + block + ', ' + '1'
            
    
        cur.execute(blockIntersect)
        blockIntersect = cur.fetchall()
        blockIntersectb = []
        for block in blockIntersect:
            block = block[0]
            blockIntersectb.append(block)
            
        # GET BLOCKS THAT ONLY OVERLAP
        blockOverlap = list(set(blockIntersectb) - set(block100b))
        print str(len(block100b)) + ' blocks are 100% covered'
        print str(len(blockOverlap)) + ' blocks are partially covered'
        
        f.close()
        
        for block in blockOverlap:
            f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'a')
            
            iOverlap = """select ST_Area(ST_Intersection(s.geom,b.geom)) /ST_Area(b.geom) FROM swat.%s s, cbpoly.block b
                            where s.gid = %s
                              and b.geoid10 = '%s'  """ % (provider,rec[0],block)

            cur.execute(iOverlap)
            iOverlap = cur.fetchone()
            iOverlap = str(iOverlap[0])
            print 'gid ' + str(rec[0]) + ' overlay with ' + block + ' block = ' + str(iOverlap) + ' overlap'            
            id=id+1
            f.writelines(str(id) + ', ' + str(rec[0]) + ', ' + rec[1] + ', ' + rec[2] + ', ' + rec[3] + ', ' + block + ', ' + iOverlap[0:8] + '\n')

        f.close()
