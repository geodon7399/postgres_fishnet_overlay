import psycopg2, time, mosaicTime

conn = psycopg2.connect("host=localhost port=5432 dbname=XXXX user=postgres password=XXXX")
cur = conn.cursor()

t = mosaicTime

#SELECT PROVIDER TABLES IN SWAT SCHEMA
providerTables = """select table_name from information_schema.tables
where table_schema = 'swat'
and table_name not like '_tmp_%'
and table_name not like '_agg_%'
and table_name not in ('grid','test_grid')
and table_name = 'verizon_lte'
order by table_name;"""

#PUT PROVIDER TABLE NAMES INTO A LIST
cur.execute(providerTables)
providerTables = cur.fetchall()

print providerTables

for provider in providerTables:
    providerTime = time.time()
    
    # CREATE A CSV FILE BY PROVIDER NAME TO SAVE DATA TO

    provider = provider[0]
    print 'Processing ' + provider + '...'
    f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.txt', 'w')
    f.close()
    
    #fix geometries of coverage
    fixGeoms = """update swat.%s
                    set geom = st_multi(st_buffer(geom,0))
                    where st_isvalid(geom) = false;""" % (provider)
    cur.execute(fixGeoms)
    #fixGeoms = cur.fetchall()
    conn.commit() 
        
    #identify number of vertices in each coverage geometry
    sProviderVertices = """select s.gid, s.mkg_name, s.entity, s.protocol
                            from swat.%s s
                            where st_npoints(s.geom) > 10000
                            --and s.gid = 21448
                            order by s.gid;""" % (provider)
    #print sProviderVertices
    cur.execute(sProviderVertices)
    sProviderVertices = cur.fetchall()
    
    #get maxGid from coverage provider. this is to insert diced polygons
    #back into the coverage table that are over 10000 vertices
    maxGid = """select max(gid) + 1 as gid from swat.%s""" % (provider)
    cur.execute(maxGid)
    maxGid = cur.fetchone()
    maxGid = int(maxGid[0])
    
    truncate = """truncate table swat.test_grid;"""
    cur.execute(truncate)
    
    print str(len(sProviderVertices)) + ' polygons > 10000 points...'
    f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.txt', 'a')
    f.writelines(str(len(sProviderVertices)) + ' polygons > 10000 points...\n')
    
    totalTime = 0
    gidLargePoly = []
    
    ########################################################DICING LARGE POLYS##################################################
    for vRec in sProviderVertices:
        start = time.time()
        print 'Dicing large polygon for ' + str(vRec) + '...'
        f.writelines('Dicing large polygon for ' + str(vRec) + '...\n')
        #gidLargePoly.append(str(vRec[0]))

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
        
        #calculate grid size for the fishnet
        xRange = (float(extent[2])-float(extent[0]))/5
        yRange = (float(extent[3])-float(extent[1]))/5
                                   
        fishnet = """insert into swat.test_grid
                (select st_multi(st_setsrid(st_createfishnet(5,5,%s,%s,%s,%s),4326)) as geom);""" % (xRange,yRange,extent[0],extent[1])
                    
        cur.execute(fishnet)
        conn.commit()
        
        fishnet = """select id from swat.test_grid"""
        cur.execute(fishnet)
        fishnet = cur.fetchall()
        fishnet = [tup[0] for tup in fishnet]
        #print str(fishnet)

        for cell in fishnet:
            #check if st_intersection returns data
            iPolyGrid = """select %s as gid, s.mkg_name,
                                  s.entity, s.protocol, st_multi(ST_Intersection(s.geom,g.geom)) as geom
                                    FROM swat.%s s, swat.test_grid g
                                      where s.gid = %s
                                        and g.id = %s
                                        and st_npoints(st_multi(ST_Intersection(s.geom,g.geom))) > 0;""" % (maxGid,provider,vRec[0],cell)
            #print iPolyGrid
            cur.execute(iPolyGrid)
            iPolyGrid = cur.fetchone()
            if iPolyGrid is not None:
                iPolyGridInsert = """insert into swat.%s
                                select %s as gid, s.mkg_name,
                                  s.entity, s.protocol, st_multi(ST_Intersection(s.geom,g.geom)) as geom
                                    FROM swat.%s s, swat.test_grid g
                                      where s.gid = %s
                                        and g.id = %s;""" % (provider,maxGid,provider,vRec[0],cell)

                #print iPolyGridInsert
                cur.execute(iPolyGridInsert)
                conn.commit()
            
            maxGid = maxGid+1
            #iGrid=iGrid+1
        
        truncate = """truncate table swat.test_grid;"""
        cur.execute(truncate)
        
        deleteLargePoly = """delete from swat.%s
                                where gid = %s;""" % (provider,vRec[0])
        cur.execute(deleteLargePoly)
        
        print 'Original large polygon ' + str(vRec[0]) + ' deleted...'
        f.writelines('Large polygon ' + str(vRec[0]) + ' deleted...\n')     
        elapsedTime = t.GetTheTime(start)
        elapsed = elapsedTime[0]
        
        print str(elapsed)
        f.writelines(str(elapsed) + '\n')
        #print gidLargePoly
        
    #####################################################END DICING LARGE POLYS##################################################

    print 'Beginning overlays for ' + provider + '...'
    f.writelines('Beginning overlays for ' + provider + '...\n')
    #create target aggregate table for coverage/block results
    createTable = """CREATE TABLE swat._tmp_%s(
                      gid serial NOT NULL,
                      mkt_name character varying(75),
                      entity character varying(75),
                      protocol character varying(75),
                      geoid10 character varying(15),
                      pct_overlap numeric,
                      
                      CONSTRAINT _tmp_%s_pkey PRIMARY KEY (gid))
                      
                    WITH (
                      OIDS=FALSE
                    );
                    
                    ALTER TABLE swat._tmp_%s
                      OWNER TO postgres;""" % (provider,provider,provider)
    cur.execute(createTable)
    conn.commit()
    
    #create a dynamic list of coverage/states
    sProviderState = """select s.gid, s.mkg_name, s.entity, s.protocol, b.state_fips 
                        from swat.%s s, base.state b
                            where st_intersects(s.geom, b.geom)
                            --and s.gid > 27716
                            group by s.gid, b.state_fips
                            order by s.gid, b.state_fips;""" % (provider)
    cur.execute(sProviderState)
    sProviderState = cur.fetchall()

    #create unique identifier in the csv file
    id=0
    f.close()
        
    for sRec in sProviderState:
        #f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'a')
        print sRec
        f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.txt', 'a')
        f.writelines(str(sRec) + '\n')
        #match gid from both lists to execute appropriate state
                
        # SELECT BLOCKS THAT ARE COVERED BY THE PROVIDER 100%
        block100 = """select b.geoid10 
                        from swat.%s s, cbpoly.block b
                        where st_contains(s.geom, b.geom)
                        and s.gid = %s 
                        and b.statefp10 = '%s'""" % (provider,sRec[0],sRec[4])
        #print block100
                    
        # SELECT BLOCKS THAT ARE < 100% COVERED BY THE PROVIDER
        blockIntersect = """select b.geoid10 
                            from swat.%s s, cbpoly.block b
                            where st_intersects(b.geom, s.geom)
                            and s.gid = %s
                            and b.statefp10 = '%s'""" % (provider,sRec[0],sRec[4])
        #print blockIntersect
            
        cur.execute(block100)
        block100 = cur.fetchall()
        block100b = []
        for block in block100:
            block = block[0]
            block100b.append(block)
            id=id+1
            insert2Tmp = """insert into swat._tmp_%s
                              values (%s,'%s','%s','%s','%s',%s);""" % (provider, id, sRec[1], sRec[2], sRec[3], block, '1')
            cur.execute(insert2Tmp)
            #f.writelines(str(id) + ', ' + str(sRec[0]) + ', ' + sRec[1] + ', ' + sRec[2] + ', ' + sRec[3] + ', ' + block + ', ' + '1\n')
                #print providerRec + ', ' + block + ', ' + '1'
        
        conn.commit()            
            
        cur.execute(blockIntersect)
        blockIntersect = cur.fetchall()
        blockIntersectb = []
        for block in blockIntersect:
            block = block[0]
            blockIntersectb.append(block)
                    
        # GET BLOCKS THAT ONLY OVERLAP
        blockOverlap = list(set(blockIntersectb) - set(block100b))
        print str(len(block100b)) + ' blocks are 100% covered'
        f.writelines(str(len(block100b)) + ' blocks are 100% covered \n')
        print str(len(blockOverlap)) + ' blocks are partially covered'
        f.writelines(str(len(blockOverlap)) + ' blocks are partially covered \n')
                
        
                
        for block in blockOverlap:
            #f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.csv', 'a')
                    
            iOverlap = """select ST_Area(ST_Intersection(s.geom,b.geom)) /ST_Area(b.geom) FROM swat.%s s, cbpoly.block b
                            where s.gid = %s
                            and b.geoid10 = '%s'  """ % (provider,sRec[0],block)
        
            cur.execute(iOverlap)
            iOverlap = cur.fetchone()
            iOverlap = str(iOverlap[0])
            print 'gid ' + str(sRec[0]) + ' overlay with ' + block + ' block = ' + str(iOverlap) + ' overlap'            
            id=id+1
            
            insert2Tmp = """insert into swat._tmp_%s
                              values (%s,'%s','%s','%s','%s',%s);""" % (provider, id, sRec[1], sRec[2], sRec[3], block, iOverlap)
            #print insert2Agg
            cur.execute(insert2Tmp)
        f.close()
        conn.commit()
    
    #create the final aggregate table
    createFinalTable = """create table swat._agg_%s as 
                            select mkt_name, entity, protocol, geoid10, sum(pct_overlap) as pct_overlap
                            from swat._tmp_%s
                            group by mkt_name, entity, protocol, geoid10;
                            
                            create sequence swat._agg_%s_seq;
                            alter table swat._agg_%s 
                            add column gid integer not null default nextval('swat._agg_%s_seq');""" % (provider,provider,provider,provider,provider)
    cur.execute(createFinalTable)
    conn.commit()
    
    #delete _aggtmp table
    deleteTmp = """drop table swat._tmp_%s;""" % (provider)
    cur.execute(deleteTmp)
    
    endProviderTime = t.GetTheTime(providerTime)
    elapsed = endProviderTime[0]
    print provider + ' completed in ' + str(elapsed)
    f = open('C:/_GEO_DATA/SWAT/mosaic/test/block_' + provider + '.txt', 'a')
    f.writelines(provider + ' completed in ' + str(elapsed) + '\n')
            
            #f.writelines(str(id) + ', ' + str(sRec[0]) + ', ' + sRec[1] + ', ' + sRec[2] + ', ' + sRec[3] + ', ' + block + ', ' + iOverlap[0:8] + '\n')
        
    f.close()
