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
    
    #create a dynamic list of coverage/states
    sProviderState = """select s.gid, b.state_fips 
                        from swat.%s s, base.state b
                            where st_intersects(s.geom, b.geom)
                            group by s.gid, b.state_fips
                            order by s.gid, b.state_fips;""" % (provider)
    cur.execute(sProviderState)
    sProviderState = cur.fetchall()
    
    #identify number of vertices in each coverage geometry
    sProviderVertices = """select s.gid, s.mkg_name, s.entity, s.protocol, st_npoints(s.geom) as numpoints
                            from swat.%s s
                            order by s.gid;""" % (provider)
    #print sProviderVertices
    cur.execute(sProviderVertices)
    sProviderVertices = cur.fetchall()
    
    #create unique identifier in the csv file
    id=0
    
    for vRec in sProviderVertices:
        f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'a')
        #print 'processing gid '+ str(vRec[0])
        print vRec
        
        #CREATE THE GRID IF THERE ARE TO MANY VERTICES
        if int(float(vRec[4])) > 20000:
            #GET EXTENT OF LARGE COVERAGE GEOMETRY
            extent = """select st_extent(geom) from swat.%s
                        where gid = %s;""" % (provider,vRec[0])
            cur.execute(extent)
            extent = cur.fetchone()
            
            extent = str(extent[0])
            extent = extent.replace('BOX(', '')
            extent = extent.replace(',', ' ')
            extent = extent.replace(')', '')
            extent = extent.split(' ')
            
            xRange = (float(extent[2])-float(extent[0]))/4
            yRange = (float(extent[3])-float(extent[1]))/4
                                   
            fishnet = """insert into swat.test_grid
                    (select st_setsrid(st_createfishnet(4,4,%s,%s,%s,%s),4326));""" % (xRange,yRange,extent[0],extent[1])
                    
            cur.execute(fishnet)
            conn.commit()
            
            fishnetIntersect = """insert into swat.test
                        (select s.gid, s.mkg_name, s.entity, s.protocol, ST_Intersection(s.geom,g.geom) 
                        FROM swat.%s s, swat.test_grid g
                        where s.gid = %s);""" % (provider,vRec[0])
            print fishnetIntersect
            
        
        
        for sRec in sProviderState:
            #match gid from both lists to execute appropriate state
            if vRec[0]==sRec[0]:
                
                # SELECT BLOCKS THAT ARE COVERED BY THE PROVIDER 100%
                block100 = """select b.geoid10 
                                from swat.%s s, cbpoly.block b
                                where st_contains(s.geom, b.geom)
                                and s.gid = %s 
                                and b.statefp10 = '%s'""" % (provider,vRec[0],sRec[1])
                #print block100
                    
                # SELECT BLOCKS THAT ARE < 100% COVERED BY THE PROVIDER
                blockIntersect = """select b.geoid10 
                                    from swat.%s s, cbpoly.block b
                                    where st_intersects(b.geom, s.geom)
                                    and s.gid = %s
                                    and b.statefp10 = '%s'""" % (provider,vRec[0],sRec[1])
                #print blockIntersect
            
                cur.execute(block100)
                block100 = cur.fetchall()
                block100b = []
                for block in block100:
                    block = block[0]
                    block100b.append(block)
                    id=id+1
                    f.writelines(str(id) + ', ' + str(vRec[0]) + ', ' + vRec[1] + ', ' + vRec[2] + ', ' + vRec[3] + ', ' + block + ', ' + '1\n')
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
                
                #f.close()
                
                for block in blockOverlap:
                    #f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'a')
                    
                    iOverlap = """select ST_Area(ST_Intersection(s.geom,b.geom)) /ST_Area(b.geom) FROM swat.%s s, cbpoly.block b
                                    where s.gid = %s
                                      and b.geoid10 = '%s'  """ % (provider,vRec[0],block)
        
                    cur.execute(iOverlap)
                    iOverlap = cur.fetchone()
                    iOverlap = str(iOverlap[0])
                    print 'gid ' + str(vRec[0]) + ' overlay with ' + block + ' block = ' + str(iOverlap) + ' overlap'            
                    id=id+1
                    f.writelines(str(id) + ', ' + str(vRec[0]) + ', ' + vRec[1] + ', ' + vRec[2] + ', ' + vRec[3] + ', ' + block + ', ' + iOverlap[0:8] + '\n')
        
        f.close()
