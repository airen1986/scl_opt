get_demand_sql = """select df.ItemId, df.LocationId, dp.PeriodStart, SUM(df.Quantity) as qty, 
                ifnull(max(ifnull(ifnull(df.SalesPrice,di.SalesPrice),dit.salesPrice)),1) as SalesPrice
                from I_ForecastOrders df,
                    O_Period dp,
                    I_InventoryPolicy di,
                    I_ItemMaster dit
                WHERE df.ForecastArrivalDate = dp.PeriodStart
                AND   df.ItemId = di.ItemId
                AND   df.LocationId = di.LocationId
                AND   df.ItemId = dit.ItemId
                GROUP BY di.ItemId, di.LocationId, dp.PeriodStart"""


get_flow_sql = """SELECT dt.ItemId, 
                dt.FromLocationId, 
                dt.ToLocationId, 
                dt.ModeId, 
                dp.PeriodStart,
                ifnull(dt.UnitTransportationCost,0) as UnitTransportationCost,
                CASE WHEN dm.TimeFrequency = 'Daily'
                        THEN CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)
                        WHEN dm.TimeFrequency = 'Weekly'
                        THEN round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/7,0)
                        WHEN dm.TimeFrequency = 'Monthly'
                        THEN round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/30,0)
                        WHEN dm.TimeFrequency = 'Quarterly'
                        THEN round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/90,0)
                        WHEN dm.TimeFrequency = 'Yearly'
                        THEN round(CAST(ifnull(dt.TransportationLeadTime,0) AS FLOAT)/360,0)
                end as TransportationLeadTime,
                ifnull(dtp.MinQuantity, ifnull(dt.MinQuantity,0)) MinQuantity,
                upper(ifnull(dtp.MaxQuantity, ifnull(dt.MaxQuantity, 'INF'))) MaxQuantity,
                ifnull(dtp.MinSplitRatio, ifnull(dt.MinSplitRatio,0)) MinSplitRatio,
                upper(ifnull(dtp.MaxSplitRatio, ifnull(dt.MaxSplitRatio, 'INF'))) MaxSplitRatio
            FROM I_TransportationPolicy dt,
                 O_Period dp,
                 I_ModelSetup dm
            LEFT JOIN I_TransportationPolicyPerPeriod dtp
            ON    dt.FromLocationId = dtp.FromLocationId
            AND    dt.ToLocationId = dtp.ToLocationId
            and    dt.ItemId= dtp.ItemId
            and    dt.ModeId = dtp.ModeId
            and    dp.PeriodStart = dtp.StartDate"""


get_inventory_sql = """SELECT di.ItemId,
                            di.LocationId,
                            dp.PeriodStart,
                            ifnull(dip.MinEndingInventory, ifnull(di.MinEndingInventory, 0)) as MinEndingInventory,
                            upper(ifnull(dip.MaxEndingInventory, ifnull(di.MaxEndingInventory, 'INF'))) as MaxEndingInventory,
                            ifnull(di.InventoryHoldingCost,ifnull(di.InventoryUnitCost, dit.UnitCost) * dm.InterestRate) as holding_cost,
                            ifnull(di.InventoryUnitCost, dit.UnitCost) as product_value
                        FROM I_InventoryPolicy di,
                            O_Period dp,
                            I_ItemMaster dit,
                            I_ModelSetup dm
                        LEFT JOIN I_InventoryPolicyPerPeriod dip
                        ON di.ItemId = dip.ItemId
                        AND di.LocationId = dip.LocationId
                        AND dp.PeriodStart = dip.StartDate
                        WHERE di.IsStorage = 1
                        and   di.ItemId = dit.ItemId"""


get_combinations_sql = """SELECT ItemId, LocationId
                        FROM I_InventoryPolicy"""

get_periods_sql = "select PeriodStart from O_Period ORDER BY 1"


get_production_sql = """SELECT di.ItemId,
                        di.LocationId,
                        dp.PeriodStart,
                        ifnull(dip.MinProductionQuantity, ifnull(di.MinProductionQuantity, 0)) as MinProductionQuantity,
                        upper(ifnull(dip.MaxProductionQuantity, ifnull(di.MaxProductionQuantity, 'INF'))) as MaxProductionQuantity
                    FROM I_InventoryPolicy di,
                        O_Period dp
                    LEFT JOIN I_InventoryPolicyPerPeriod dip
                    ON di.ItemId = dip.ItemId
                    AND di.LocationId = dip.LocationId
                    AND dp.PeriodStart = dip.StartDate
                    WHERE di.IsProduction = 1"""

get_resources_sql = """SELECT dr.ResourceId,
                    dr.LocationId,
                    dp.PeriodStart,
                    upper(ifnull(drp.SupplyCapacity, ifnull(dr.SupplyCapacity, 'INF'))) as SupplyCapacity,
                    ifnull(drp.MinUtilization, ifnull(dr.MinUtilization, 'INF')) as MinUtilization,
                    upper(ifnull(drp.MaxUtilization, ifnull(dr.MaxUtilization, 'INF'))) as MaxUtilization
                FROM I_ResourceMaster dr,
                    O_Period dp
                LEFT JOIN I_ResourcePerPeriod drp
                on dr.ResourceId = drp.ResourceId
                and dp.PeriodStart = drp.StartDate"""

get_processes_sql = """select   dop.ItemId, 
                            dop.LocationId, 
                            dop.ProcessId,  
                            dp.PeriodStart,
                            MAX(ifnull(dpp.MinSplitRatio, ifnull(dop.MinSplitRatio,0))) MinSplitRatio,
                            MIN(upper(ifnull(dpp.MaxSplitRatio, ifnull(dop.MaxSplitRatio, 'INF')))) MaxSplitRatio,
                            SUM(ifnull(dop.UnitOperationCost,0))
                    from I_Processes dop,
                        O_Period dp
                    LEFT JOIN I_ProcessesPerPeriod dpp
                    ON dop.ItemId = dpp.ItemId
                    AND dop.LocationId = dpp.LocationId
                    and dop.ProcessId = dpp.ProcessId
                    and dop.ProcessStep = dpp.PRocessStep
                    and dp.PeriodStart = dpp.StartDate
                    GROUP BY dop.ItemId, 
                             dop.LocationId, 
                             dop.ProcessId,
                             dp.PeriodStart"""

get_bom_sql = """SELECT         dop.ProcessId,
                            dop.ItemId AS ToItemId,
                            dop.LocationId,
                            db.ItemId AS FromItemId,
                            dp.PeriodStart,
                            db.usageQuantity,
                            ifnull(dpp.yield, ifnull(dop.Yield,1)) AS yield
            FROM I_Processes dop,
                I_BOMRecipe db,
                O_Period dp
            LEFT JOIN I_ProcessesPerPeriod dpp 
            ON dop.ItemId = dpp.ItemId AND 
            dop.LocationId = dpp.LocationId AND 
            dop.ProcessId = dpp.ProcessId AND 
            dop.ProcessStep = dpp.ProcessStep AND 
            dp.PeriodStart = dpp.StartDate
            WHERE dop.BOMId = db.BOMId AND 
                dop.LocationId = db.LocationId"""

get_resource_constraint_sql = """SELECT DISTINCT dop.ProcessId,
                                            dop.ProcessStep,
                                            dop.ItemId,
                                            dop.LocationId,
                                            dp.PeriodStart,
                                            dop.ResourceId,
                                            ifnull(dpp.yield, ifnull(dop.Yield,1)) as yield,
                                            ifnull(ifnull(dpp.UnitOperationTime, dop.UnitOperationTime),0) as UnitOperationTime
                            FROM I_Processes dop,
                                O_Period dp
                            LEFT JOIN I_ProcessesPerPeriod dpp 
                            ON dop.ItemId = dpp.ItemId AND 
                            dop.LocationId = dpp.LocationId AND 
                            dop.ProcessId = dpp.ProcessId AND 
                            dop.ProcessStep = dpp.PRocessStep AND 
                            dp.PeriodStart = dpp.StartDate """


reg_demand_sql = """select dr.ItemId, dr.LocationId, dp.PeriodStart, dr.ForecastItemId, SUM(Quantity) as qty
                from I_ForecastOrders df,
                    I_ForecastRegistration dr,
                    O_Period dp
                WHERE df.ItemId = dr.ForecastItemId
                and   df.LocationId = dr.LocationId
                AND   df.ForecastArrivalDate = dp.PeriodStart
                and   df.ForecastArrivalDate >= dr.StartDate
                and   df.ForecastArrivalDate <= dr.EndDate
                GROUP BY dr.ItemId, dr.LocationId, dp.PeriodStart, dr.ForecastItemId"""


update_entry_date_sql = """UPDATE I_OpeningStocks
                        SET EntryDate = I_ModelSetup.StartDate
                        FROM I_ModelSetup
                        WHERE IFNULL(EntryDate, I_ModelSetup.StartDate) <= I_ModelSetup.StartDate;"""

initial_inv_sql = """SELECT ds.ItemId, ds.LocationId, 
                    ifnull(ds.EntryDate, prd.PeriodStart),
                    ROUND(SUM(ds.Quantity),5) as ttl
                    FROM I_OpeningStocks ds,
                        I_InventoryPolicy di,
                        O_Period dp,
                        (
                            select Min(PeriodStart) as PeriodStart, Max(PeriodStart) as PeriodEnd 
                            from O_Period
                        ) prd
                    WHERE di.IsStorage = 1
                    AND   ds.ItemId = di.ItemId
                    and   ds.LocationId = di.LocationId
                    and   ifnull(ds.EntryDate, prd.PeriodStart) = dp.PeriodStart
                    AND   ifnull(ds.ExpiryDate, prd.PeriodEnd) > ifnull(ds.EntryDate, prd.PeriodStart)
                    GROUP BY ds.ItemId, ds.LocationId, ds.EntryDate"""

expiry_inv_sql = """SELECT ds.ItemId, ds.LocationId, 
                        ifnull(ds.EntryDate, prd2.PeriodStart) as EntryDate,
                        prd.PeriodStart as ExpiryDate,
                    ROUND(SUM(ds.Quantity),5) as ttl
                    FROM I_OpeningStocks ds,
                        I_InventoryPolicy di,
                        O_Period dp,
                        (
                            SELECT dp1.PeriodStart, Min(dp2.PeriodStart) as PeriodEnd
                            FROM O_Period dp1,
                                 O_Period dp2
                            WHERE dp2.PEriodStart > dp1.PeriodStart
                            GROUP BY dp1.PeriodStart
                        ) prd,
                        (
                            select Min(PeriodStart) as PeriodStart, Max(PeriodStart) as PeriodEnd 
                            from O_Period
                        ) prd2
                    WHERE di.IsStorage = 1
                    AND   ds.ItemId = di.ItemId
                    and   ds.LocationId = di.LocationId
                    and   ifnull(ds.EntryDate, prd2.PeriodStart) = dp.PeriodStart
                    AND   ds.ExpiryDate > ifnull(ds.EntryDate, prd2.PeriodStart)
                    AND   ds.ExpiryDate >= prd.PeriodStart
                    AND   ds.ExpiryDate < prd.PeriodEnd
                    AND   ds.ExpiryDate is not null
                    GROUP BY ds.ItemId, ds.LocationId, ifnull(ds.EntryDate, prd2.PeriodStart), prd.PeriodStart"""


ss_sql = """select  di.ItemId, 
                    di.LocationId,
                    dp.PeriodStart,
                    dp.PeriodIndex,
                    CASE WHEN TimeFrequency = 'Daily'
                    THEN di.DOSWindow
                    WHEN TimeFrequency = 'Weekly'
                    THEN round(di.DOSWindow/7,0)
                    WHEN TimeFrequency = 'Monthly'
                    THEN round(di.DOSWindow/30,0)
                    WHEN TimeFrequency = 'Quarterly'
                    THEN round(di.DOSWindow/90,0)
                    WHEN TimeFrequency = 'Yearly'
                    THEN round(di.DOSWindow/360,0)
                    end as DOSWindow,
                    CASE WHEN TimeFrequency = 'Daily'
                    THEN ifnull(dip.SafetyStockDOS,di.SafetyStockDOS)
                    WHEN TimeFrequency = 'Weekly'
                    THEN round(CAST(ifnull(dip.SafetyStockDOS,di.SafetyStockDOS) AS FLOAT)/7,4)
                    WHEN TimeFrequency = 'Monthly'
                    THEN round(CAST(ifnull(dip.SafetyStockDOS,di.SafetyStockDOS) AS FLOAT)/30,4)
                    WHEN TimeFrequency = 'Quarterly'
                    THEN round(CAST(ifnull(dip.SafetyStockDOS,di.SafetyStockDOS) AS FLOAT)/90,4)
                    WHEN TimeFrequency = 'Yearly'
                    THEN round(CAST(ifnull(dip.SafetyStockDOS,di.SafetyStockDOS) AS FLOAT)/360,4)
                    end as SafetyStockDOS
            from I_InventoryPolicy di,
                 I_ModelSetup dm,
                 (    select PeriodStart, row_number() over (order by PeriodStart) - 1 as PeriodIndex
                      from O_Period
                 ) dp
            LEFT JOIN I_InventoryPolicyPerPeriod dip
            ON di.ItemId = dip.ItemId
            AND di.LocationId = dip.LocationId
            AND dp.PeriodStart = dip.StartDate
            WHERE di.IsStorage = 1
            AND   ifnull(dip.SafetyStockDOS, ifnull(di.SafetyStockDOS, 0)) > 0"""


registration_ss = """SELECT di2.ItemId,
                        di2.LocationId,
                        dp.PeriodStart,
                        CASE WHEN TimeFrequency = 'Daily'
                                THEN di1.DOSWindow
                                WHEN TimeFrequency = 'Weekly'
                                THEN round(di1.DOSWindow/7,0)
                                WHEN TimeFrequency = 'Monthly'
                                THEN round(di1.DOSWindow/30,0)
                                WHEN TimeFrequency = 'Quarterly'
                                THEN round(di1.DOSWindow/90,0)
                                WHEN TimeFrequency = 'Yearly'
                                THEN round(di1.DOSWindow/360,0)
                        end as DOSWindow,
                        max(CASE WHEN TimeFrequency = 'Daily'
                            THEN di1.SafetyStockDOS
                            WHEN TimeFrequency = 'Weekly'
                            THEN round(CAST(di1.SafetyStockDOS AS FLOAT)/7,4)
                            WHEN TimeFrequency = 'Monthly'
                            THEN round(CAST(di1.SafetyStockDOS AS FLOAT)/30,4)
                            WHEN TimeFrequency = 'Quarterly'
                            THEN round(CAST(di1.SafetyStockDOS AS FLOAT)/90,4)
                            WHEN TimeFrequency = 'Yearly'
                            THEN round(CAST(di1.SafetyStockDOS AS FLOAT)/360,4)
                            end) as SafetyStockDOS
                    FROM I_ForecastRegistration dr,
                        I_InventoryPolicy di1,
                        I_InventoryPolicy di2,
                        O_Period dp,
                        I_ModelSetup dm
                    WHERE 1 = 1
                    and   dr.ForecastItemId = di1.ItemId
                    and   dr.LocationId = di1.LocationId
                    and   dr.ItemId = di2.ItemId
                    and   dr.LocationId = di2.LocationId
                    AND   di1.IsStorage = 1
                    and   dp.PeriodStart >= dr.StartDate
                    and   dp.PeriodStart <= dr.EndDate
                    GROUP BY di2.ItemId,
                            di2.LocationId,
                            dp.PeriodStart"""


min_release_sql = """SELECT di.ItemId,
                            di.LocationId,
                            dp.PeriodStart,
                            dp.PeriodIndex,
                            CASE WHEN dm.TimeFrequency = 'Daily'
                                THEN di.MinReleaseTime
                                WHEN dm.TimeFrequency = 'Weekly'
                                THEN round(CAST(di.MinReleaseTime AS FLOAT)/7,4)
                                WHEN dm.TimeFrequency = 'Monthly'
                                THEN round(CAST(di.MinReleaseTime AS FLOAT)/30,4)
                                WHEN dm.TimeFrequency = 'Quarterly'
                                THEN round(CAST(di.MinReleaseTime AS FLOAT)/90,4)
                                WHEN dm.TimeFrequency = 'Yearly'
                                THEN round(CAST(di.MinReleaseTime AS FLOAT)/360,4)
                                end as MinReleaseTime
                        FROM I_InventoryPolicy di,
                            I_ModelSetup dm,
                            (    select PeriodStart, row_number() over (order by PeriodStart) - 1 as PeriodIndex
                                            from O_Period
                            ) dp
                        WHERE di.IsStorage = 1
                        AND  ifnull(di.MinReleaseTime, 0) > 0"""

get_stocking_locations_sql = """select I_ItemMaster.ItemId, LocationId, 
                                    ifnull(Max(InventoryUnitCost), I_ItemMaster.UnitCost) as UnitCostModel
                                from I_InventoryPolicy, I_ItemMaster
                                WHERE IsStorage = 1
                                and   I_ItemMaster.ItemId = I_InventoryPolicy.ItemId
                                GROUP BY I_ItemMaster.ItemId, LocationId"""