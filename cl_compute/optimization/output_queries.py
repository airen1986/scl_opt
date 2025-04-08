
flow_sql = """insert into O_Transportation (ItemId, FromLocationId, ToLocationId,
                                    StartDate, ModeId, FlowQuantity )
                 Values (?, ?, ?, ?, ?, ?)"""

production_sql = """insert into O_Production (ItemId, LocationId, ProcessId,
                        StartDate,  ProductionQuantity) Values ( ?, ?, ?, ?, ?)"""

inventory_sql = """insert into O_Inventory (ItemId, LocationId, StartDate,  
                        OpeningInventory, EndingInventory, ShortFallInventory, InboundStock, OutboundStock,
                         ProductionQuantity, SatisfiedDemand, Demand, RegistrationInbound, 
                         RegistrationOutbound, ConsumedQuantity, RequiredInventory, IncomingStock, 
                         InTransitInventory, InReleaseInventory, ExpiredQuanity ) 
                        Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?)"""

initial_inv_sql = """insert into O_InitialInventory (ItemId, LocationId, Quantity) 
                        Values (?, ?, ?)"""

reg_sql = """insert into O_ForecastRegistration (ItemId, LocationId, StartDate,  
                        ForecastItemId, SatisfiedQuantity) 
                        Values (?, ?, ?, ?, ?)"""

update_period_end = """UPDATE O_Transportation
                        set EndDate = date(StartDate, TransportationLeadTime)
                        FROM 
                        (
                       select df.rowid as rid,
                            CASE WHEN dm.TimeFrequency = 'Daily'
                                    THEN  '+'||(round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT), 0))|| ' days'
                                    WHEN dm.TimeFrequency = 'Weekly'
                                    THEN '+'||(round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/7,0) * 7)|| ' days'
                                    WHEN dm.TimeFrequency = 'Monthly'
                                    THEN '+'||(round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/30,0) * 1)|| ' months'
                                    WHEN dm.TimeFrequency = 'Quarterly'
                                    THEN '+'||(round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/90,0) * 3)|| ' months'
                                    WHEN dm.TimeFrequency = 'Yearly'
                                    THEN '+'||(round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/360,0) * 12)|| ' months'
                            END as TransportationLeadTime
                        from O_Transportation df,
                            I_TransportationPolicy dt,
                            I_ModelSetup dm
                        WHERE df.ItemId = dt.ItemId
                        and   df.FromLocationId = dt.FromLocationId
                        and   df.ToLocationId = dt.ToLocationId
                        and   df.ModeId = dt.ModeId
                        ) t1
                        WHERE O_Transportation.rowid = t1.rid;"""


update_intransit_inventory = """UPDATE O_Inventory
                                Set InTransitInventory = t1.Qty
                                FROM (
                                    select  ItemId, 
                                            ToLocationId,
                                            dp.PeriodStart,
                                            sum(FlowQuantity) as Qty  
                                    from O_Transportation df,
                                         O_Period dp
                                    WHERE dp.PeriodStart >= df.StartDate
                                    and   dp.PeriodStart < df.EndDate
                                    GROUP BY ItemId, 
                                            ToLocationId,
                                            dp.PeriodStart
                                ) t1
                                WHERE O_Inventory.ItemId = t1.ItemId
                                and   O_Inventory.LocationId = t1.ToLocationId
                                and   O_Inventory.StartDate = t1.PeriodStart;"""

update_ordered_quantity = """UPDATE O_Inventory Set OrderedQuantity = 0;
                                UPDATE O_Inventory
                                Set OrderedQuantity = t1.Qty
                                FROM (
                                    select  ItemId, 
                                            ToLocationId,
                                            StartDate,
                                            sum(FlowQuantity) as Qty  
                                    from O_Transportation df
                                    GROUP BY ItemId, 
                                            ToLocationId,
                                            StartDate
                                ) t1
                                WHERE O_Inventory.ItemId = t1.ItemId
                                and   O_Inventory.LocationId = t1.ToLocationId
                                and   O_Inventory.StartDate = t1.StartDate"""

update_inrelease_inventory = """UPDATE O_Inventory
                                Set InReleaseInventory = t1.Qty
                                FROM (
                                    SELECT  ItemId,
                                            LocationId,
                                            dp.PeriodStart as StartDate,
                                            SUM(ProductionQuantity) as Qty
                                        from
                                        (
                                            SELECT  oi.ItemId,
                                                    oi.LocationId,
                                                    oi.StartDate,
                                                    date(oi.StartDate, '+'||
                                                    ROUND(CASE WHEN dm.TimeFrequency = 'Daily'
                                                    THEN di.MinReleaseTime
                                                    WHEN dm.TimeFrequency = 'Weekly'
                                                    THEN round(CAST(di.MinReleaseTime AS FLOAT)/7,4)*7
                                                    WHEN dm.TimeFrequency = 'Monthly'
                                                    THEN round(CAST(di.MinReleaseTime AS FLOAT)/30,4)*30
                                                    WHEN dm.TimeFrequency = 'Quarterly'
                                                    THEN round(CAST(di.MinReleaseTime AS FLOAT)/90,4)*90
                                                    WHEN dm.TimeFrequency = 'Yearly'
                                                    THEN round(CAST(di.MinReleaseTime AS FLOAT)/360,4)*360
                                                    end, 0)||' days') as EndDate,
                                                    oi.ProductionQuantity
                                            FROM O_Inventory oi,
                                                I_InventoryPolicy di,
                                                I_ModelSetup dm
                                            WHERE oi.ItemId = di.ItemId
                                            and   oi.LocationId = di.LocationId
                                            and   oi.ProductionQuantity > 0
                                            and   ifnull(di.MinReleaseTime,0) > 0
                                            ) t1, O_Period dp
                                            WHERE dp.PeriodStart >= t1.StartDate
                                            and   dp.PeriodStart < t1.EndDate
                                            GROUP BY ItemId,
                                                LocationId,
                                                dp.PeriodStart
                                ) t1
                                WHERE O_Inventory.ItemId = t1.ItemId
                                and   O_Inventory.LocationId = t1.LocationId
                                and   O_Inventory.StartDate = t1.StartDate;"""

update_production_cost = """UPDATE O_Production
                            Set ProductionCost = ProductionQuantity * t1.UnitOperationCost
                            FROM 
                                (
                                    SELECT ProcessId, ItemId, LocationId, SUM(ifnull(UnitOperationCost,0)) UnitOperationCost
                                    From I_Processes
                                    GROUP BY ProcessId, ItemId, LocationId
                                ) t1
                            WHERE O_Production.ItemId = t1.ItemId
                            AND   O_Production.LocationId = t1.LocationId
                            AND   O_Production.ProcessId = t1.ProcessId"""

update_transportation_cost = """UPDATE O_Transportation
                                set FlowTransportationCost = FlowQuantity * ifnull(I_TransportationPolicy.UnitTransportationCost,0)
                                FROM I_TransportationPolicy
                                WHERE O_Transportation.ItemId = I_TransportationPolicy.ItemId
                                AND   O_Transportation.FromLocationId = I_TransportationPolicy.FromLocationId
                                AND   O_Transportation.ToLocationId = I_TransportationPolicy.ToLocationId
                                AND   O_Transportation.ModeId = I_TransportationPolicy.ModeId"""