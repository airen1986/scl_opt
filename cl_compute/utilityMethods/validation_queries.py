insert_query = "INSERT INTO O_ModelValidation (TableName, ColumnName, ColumnValue, ErrorType, ErrorMsg)"

primary_keys = {
                'I_ItemMaster': ['ItemId'],
                'I_LocationMaster': ['LocationId'],
                'I_ModelSetup': ['ModelName'],
                'I_ForecastOrders': ['OrderId'],
                'I_OpeningStocks':	['StockId'],
                'I_InventoryPolicy': ['ItemId', 'LocationId'],
                'I_InventoryPolicyPerPeriod': ['ItemId', 'LocationId', 'StartDate'],
                'I_Processes':	['ProcessId', 'ProcessStep'],
                'I_ProcessesPerPeriod': ['ProcessId', 'ProcessStep', 'StartDate'],
                'I_ResourceMaster':	['ResourceId'],
                'I_ResourcePerPeriod':	['ResourceId', 'StartDate'],
                'I_BOMRecipe': ['BomId', 'ItemId', 'LocationId'],
                'I_TransportationPolicy': ['ItemId', 'FromLocationId', 'ToLocationId', 'ModeId'],
                'I_TransportationPolicyPerPeriod': ['ItemId', 'FromLocationId', 'ToLocationId', 'ModeId', 'StartDate'],
                'I_ForecastRegistration': [ 'ForecastItemId', 'LocationId', 'ItemId', 'StartDate'],
                }
             
foreign_keys = [    [{'I_InventoryPolicy': ('ItemId',)}, {'I_ItemMaster': ('ItemId',)}],
                    [{'I_InventoryPolicy': ('LocationId',)}, {'I_LocationMaster': ('LocationId',)}],
                    [{'I_BOMRecipe': ('BOMId', 'LocationId')}, {'I_Processes': ('BOMId', 'LocationId')}],
                    [{'I_BOMRecipe': ('ItemId', 'LocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_ForecastOrders': ('ItemId', 'LocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_ForecastOrders': ('ForecastArrivalDate',)}, {'O_Period': ('PeriodStart',)}],
                    [{'I_InventoryPolicyPerPeriod': ('ItemId', 'LocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_InventoryPolicyPerPeriod': ('StartDate',)}, {'O_Period': ('PeriodStart',)}],
                    [{'I_Processes': ('ItemId', 'LocationId')}, {'I_InventoryPolicy':('ItemId', 'LocationId')}],
                    [{'I_Processes': ('ResourceId', 'LocationId')}, {'I_ResourceMaster': ('ResourceId', 'LocationId')}],
                    [{'I_Processes': ('BOMId',)}, {'I_BOMRecipe': ('BOMId',)}],
                    [{'I_ProcessesPerPeriod': ('ItemId', 'LocationId', 'ProcessId', 'ProcessStep')}, 
                        {'I_Processes': ('ItemId', 'LocationId', 'ProcessId', 'ProcessStep')}],
                    [{'I_ProcessesPerPeriod': ('StartDate',)}, {'O_Period': ('PeriodStart',)}],
                    [{'I_ResourceMaster': ('ResourceId', )}, {'I_Processes': ('ResourceId', )}],
                    [{'I_ResourcePerPeriod': ('ResourceId', )}, {'I_ResourceMaster': ('ResourceId', )}],
                    [{'I_ResourcePerPeriod': ('StartDate',)}, {'O_Period': ('PeriodStart',)}],
                    [{'I_OpeningStocks': ('ItemId', 'LocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_OpeningStocks': ('EntryDate',)}, {'O_Period': ('PeriodStart',)}],
                    [{'I_TransportationPolicy': ('ItemId', 'FromLocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_TransportationPolicy': ('ItemId', 'ToLocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_TransportationPolicyPerPeriod': ('ItemId', 'FromLocationId', 'ToLocationId', 'ModeId')}, 
                        {'I_TransportationPolicy': ('ItemId', 'FromLocationId', 'ToLocationId', 'ModeId')}],
                    [{'I_TransportationPolicyPerPeriod': ('StartDate',)}, {'O_Period': ('PeriodStart',)}],
                    [{'I_ForecastRegistration': ('ForecastItemId', 'LocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_ForecastRegistration': ('ItemId', 'LocationId')}, {'I_InventoryPolicy': ('ItemId', 'LocationId')}],
                    [{'I_ForecastRegistration': ('StartDate',)}, {'O_Period': ('PeriodStart',)}]]


min_max_constraints = [ ('I_InventoryPolicy',  'MinEndingInventory',   'MaxEndingInventory'),
                        ('I_InventoryPolicy', 'MinProductionQuantity', 'MaxProductionQuantity'),
                        ('I_InventoryPolicy', 'InventoryUnitCost', 'SalesPrice'),
                        ('I_InventoryPolicyPerPeriod',  'MinEndingInventory',   'MaxEndingInventory'),
                        ('I_InventoryPolicyPerPeriod', 'MinProductionQuantity', 'MaxProductionQuantity'),
                        ('I_Processes', 'MinSplitRatio', 'MaxSplitRatio'),
                        ('I_ProcessesPerPeriod', 'MinSplitRatio', 'MaxSplitRatio'),
                        ('I_ResourceMaster', 'MinUtilization', 'MaxUtilization'),
                        ('I_ResourcePerPeriod', 'MinUtilization', 'MaxUtilization'),
                        ('I_TransportationPolicy', 'MinQuantity', 'MaxQuantity'),
                        ('I_TransportationPolicy', 'MinSplitRatio', 'MaxSplitRatio'),
                        ('I_TransportationPolicyPerPeriod', 'MinQuantity', 'MaxQuantity'),
                        ('I_TransportationPolicyPerPeriod', 'MinSplitRatio', 'MaxSplitRatio'),
                        ('I_ForecastRegistration','StartDate', 'EndDate'),
                        ('I_OpeningStocks', 'EntryDate', 'ExpiryDate')]

positive_vals =         {    
                            'I_BOMRecipe': ['UsageQuantity'],
                            'I_ForecastOrders': ['Quantity', 'SalesPrice'],
                            'I_InventoryPolicy': ['InventoryUnitCost'],
                            'I_Processes': ['UnitOperationTime', 'Yield'],
                            'I_ModelSetup': ['NumberOfPeriods', 'InterestRate']
                        }

null_or_positive =  {
                        'I_ProcessesPerPeriod': ['UnitOperationTime', 'Yield']
                    }

non_negative_vals = {   'I_InventoryPolicy': ['MinEndingInventory', 'MaxEndingInventory', 'MinProductionQuantity', 
                            'MaxProductionQuantity','InventoryUnitCost', 'InventoryHoldingCost', 'SalesPrice', 
                            'SafetyStockDOS', 'DOSWindow', 'InventoryShelfLife'],
                        'I_InventoryPolicyPerPeriod': ['MinEndingInventory', 'MaxEndingInventory', 'MinProductionQuantity', 
                                                       'MaxProductionQuantity'],
                        'I_ItemMaster': ['SalesPrice', 'UnitCost'],
                        'I_Processes': ['UnitOperationCost', 'MOQ', 'MinSplitRatio'],
                        'I_ProcessesPerPeriod': ['MinSplitRatio'],
                        'I_ResourceMaster': ['SupplyCapacity', 'MinUtilization'],
                        'I_ResourcePerPeriod': ['SupplyCapacity', 'MinUtilization'],
                        'I_TransportationPolicy': ['UnitTransportationCost', 'TransportationLeadTime', 'MinQuantity', 
                                             'MaxQuantity',  'MinSplitRatio', 'MaxSplitRatio'],
                        'I_TransportationPolicyPerPeriod': ['MinQuantity', 'MinSplitRatio']
                    }

max_1_values = {    'I_ModelSetup': ['InterestRate'],
                    'I_Processes': ['Yield', 'MinSplitRatio', 'MaxSplitRatio'],
                    'I_ProcessesPerPeriod': ['Yield', 'MinSplitRatio', 'MaxSplitRatio'],
                    'I_ResourceMaster': ['MinUtilization', 'MaxUtilization'],
                    'I_ResourcePerPeriod': ['MinUtilization', 'MaxUtilization'],
                    'I_TransportationPolicy': ['MinSplitRatio', 'MaxSplitRatio'],
                    'I_TransportationPolicyPerPeriod': ['MinSplitRatio', 'MaxSplitRatio']
                }


boolean_values = {  
                    'I_InventoryPolicy': ['IsProduction', 'IsStorage', 'InventoryStatus'],
                    'I_ItemMaster': ['ItemStatus'],
                #    'I_ModelSetup':['DOSWindowStartPeriod'],
                    'I_ResourceMaster': ['ResourceStatus']
                }


no_source_query = """WITH T1 AS
                        (
                            SELECT DISTINCT dop.BOMId, dop.LocationId, dop.ItemId to_item, db.ItemId from_item
                            FROM I_Processes dop,
                                I_BOMRecipe db,
                                I_InventoryPolicy di
                            WHERE dop.BOMId = db.BOMId
                            AND   di.ItemId = db.ItemId
                            and   di.LocationId = db.LocationId
                            and   di.IsProduction = 0
                        )
                        SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||T1.from_item||','||T1.LocationId||')',
                                'Error',
                                'No Transportation/Manufacturing policy to this destination' 
                        FROM T1
                        LEFT JOIN I_TransportationPolicy
                        ON    T1.from_item = I_TransportationPolicy.ItemId
                        and   T1.LocationId = I_TransportationPolicy.ToLocationId
                        LEFT JOIN I_TransportationPolicy t2
                        ON    T1.from_item = t2.ItemId
                        and   T1.LocationId = t2.FromLocationId
                        WHERE I_TransportationPolicy.FromLocationId IS NULL
                        AND   t2.ToLocationId is not NULL
                        UNION
                        SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||di.ItemId||','||di.LocationId||')',
                                'Error',
                                'No Transportation/Manufacturing policy to this destination'
                        FROM
                        (
                            select t1.ItemId, t1.FromLocationId, t1.ToLocationId
                            from I_TransportationPolicy t1
                            LEFT JOIN I_TransportationPolicy t2
                            ON t1.ItemId = t2.ItemId
                            and t1.FromLocationId = t2.ToLocationId
                            WHERE t2.ToLocationId is NULL
                        ) t3,
                        I_InventoryPolicy di
                        WHERE t3.ItemId = di.ItemId
                        and   t3.FromLocationId = di.LocationId
                        and   di.IsProduction = 0
                        UNION
                        SELECT DISTINCT 'I_ForecastOrders',
                                        '(ItemId, LocationId)',
                                        '(' || df.ItemId || ',' || df.LocationId || ')',
                                        'Error',
                                        'No Transportation/Manufacturing policy for this Forecast Order Item'
                        FROM (
                                SELECT DISTINCT ifnull(I_ForecastRegistration.ItemId, I_ForecastOrders.ItemId) AS ItemId,
                                                I_ForecastOrders.LocationId
                                FROM I_ForecastOrders
                                        LEFT JOIN
                                        I_ForecastRegistration ON I_ForecastOrders.ItemId = I_ForecastRegistration.ForecastItemId AND 
                                                                I_ForecastOrders.LocationId = I_ForecastRegistration.LocationId AND 
                                                                I_ForecastOrders.ForecastArrivalDate >= I_ForecastRegistration.StartDate AND 
                                                                I_ForecastOrders.ForecastArrivalDate <= I_ForecastRegistration.EndDate
                                WHERE I_ForecastOrders.Quantity > 0
                        )
                        df
                        LEFT JOIN
                        I_TransportationPolicy dt ON df.ItemId = dt.ItemId AND 
                                                df.LocationId = dt.toLocationId
                        LEFT JOIN
                        (
                                SELECT ItemId, LocationId
                                FROM I_InventoryPolicy
                                WHERE IsProduction = 1
                        )k2 
                        ON  df.ItemId = k2.ItemId AND 
                            df.LocationId = k2.LocationId
                        WHERE dt.fromLocationId IS NULL AND 
                        k2.LocationId IS NULL"""

no_process_code = """SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||d1.ItemId||','||d1.LocationId||')',
                                'Error',
                                'No Process in I_Processes Table'
                        from I_InventoryPolicy d1
                        LEFT JOIN I_Processes d2
                        ON d1.ItemId = d2.ItemId
                        and d1.LocationId = d2.LocationId
                        WHERE d1.IsProduction = 1
                        and   d2.ProcessId is NULL"""

duplicate_processes = """SELECT  'I_Processes', 
                            'ProcessId',
                            ProcessId,
                            'Error',
                            'ProcessId associated with multiple Item, Locations'
                    FROM I_Processes
                    GROUP BY ProcessId
                    having count(DISTINCT ItemId||LocationId) > 1
                    UNION
                    SELECT  'I_Processes', 
                            'BOMId',
                            BOMId,
                            'Error',
                            'BOMId associated with multiple Item, Locations'
                    FROM I_Processes
                    WHERE BOMId is not null
                    GROUP BY BOMId
                    having count(DISTINCT ItemId||LocationId) > 1
                    UNION
                    SELECT  'I_Processes', 
                            'ProcessId',
                            ProcessId,
                            'Error',
                            "ProcessId associated with multiple BOM's"
                    from I_Processes
                    group by ProcessId
                    HAVING count(distinct BOMId) > 1
                    UNION
                    SELECT  'I_BOMRecipe', 
                            'BOMId',
                            BOMId,
                            'Error',
                            'BOMId associated with multiple Locations'
                    from I_BOMRecipe
                    GROUP BY BOMId
                    HAVING COUNT(Distinct LocationId)  > 1"""

sourcing_warning = """ SELECT  'I_Processes', 
                                '(ItemId, LocationId)',
                                '('||dop.ItemId||','||dop.LocationId||')',
                                'Warning',
                                'This record has both make and inbound transportation policies'
                        FROM I_Processes dop
                        LEFT JOIN I_TransportationPolicy dt
                        ON dop.ItemId = dt.ItemId
                        and dop.LocationId = dt.ToLocationId
                        WHERE dt.FromLocationId is not null
                        UNION
                        SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||ItemId||','||LocationId||')',
                                'Warning',
                                'This record has no source/destination policies'
                        FROM
                        (
                            SELECT di.ItemId, di.LocationId
                            FROM I_InventoryPolicy di
                            LEFT JOIN I_TransportationPolicy dt
                            ON di.ItemId = dt.ItemId
                            and di.LocationId = dt.ToLocationId
                            WHERE dt.FromLocationId is  null
                            EXCEPT
                            SELECT dop.ItemId, dop.LocationId
                            FROM I_Processes dop
                            EXCEPT
                            SELECT DISTINCT ForecastItemId, LocationId
                            FROM I_ForecastRegistration
                            EXCEPT
                            select distinct ItemId, LocationId
                            from I_OpeningStocks
                            WHERE Quantity > 0
                        )"""

dos_window_error = """ SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||ItemId||','||LocationId||')',
                                'DOSWindow Error',
                                'DOSWindow should be multiple of: '||period_days
                        FROM
                        (
                                SELECT  I_InventoryPolicy.ItemId, 
                                        I_InventoryPolicy.LocationId,
                                        I_InventoryPolicy.SafetyStockDOS, 
                                        I_InventoryPolicy.DOSWindow, 
                                        I_ModelSetup.TimeFrequency,
                                        CASE I_ModelSetup.TimeFrequency
                                        WHEN 'Weekly' THEN 7
                                        WHEN 'Monthly' THEN 30
                                        WHEN 'Quarterly' THEN 90
                                        WHEN 'Yearly' THEN 365 END AS period_days
                                from I_InventoryPolicy,
                                I_ModelSetup
                                WHERE ifnull(I_InventoryPolicy.SafetyStockDOS,0) > 0
                                and   ifnull(I_InventoryPolicy.DOSWindow,0) > 0
                        )
                        WHERE MOd(DOSWindow, period_days) > 0
                        UNION
                        SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||ItemId||','||LocationId||')',
                                'DOSWindow Error',
                                'DOSWindow cannot be empty or zero with nonzero safety stock'
                        FROM I_InventoryPolicy
                        WHERE ifnull(I_InventoryPolicy.SafetyStockDOS,0) > 0
                        and   ifnull(I_InventoryPolicy.DOSWindow,0) = 0
                        UNION
                        SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||ItemId||','||LocationId||')',
                                'MinEndingInventory Error',
                                'MinEndingInventory cannot be non zero for non stocking locations' 
                        FROM I_InventoryPolicy
                        WHERE IsStorage = 0
                        and  IFNULL(MinEndingInventory,0) > 0
                        UNION
                        SELECT  'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||ItemId||','||LocationId||')',
                                'MinReleaseTime Error',
                                'MinReleaseTime cannot be non zero for non stocking locations' 
                        FROM I_InventoryPolicy
                        WHERE IsStorage = 0
                        and  IFNULL(MinReleaseTime,0) > 0
                        UNION
                        SELECT 'I_InventoryPolicy', 
                                '(ItemId, LocationId)',
                                '('||ItemId||','||LocationId||')',
                                'SafetyStockDOS Error',
                                'SafetyStockDOS cannot be non zero for non stocking locations' 
                        FROM I_InventoryPolicy
                        WHERE IsStorage = 0
                        and IFNULL(SafetyStockDOS,0) > 0
                        UNION
                        SELECT 'I_OpeningStocks', 
                                '(ItemId, LocationId)',
                                '('||ds.ItemId||','||ds.LocationId||')',
                                'Stock Error',
                                'Cannot held stock for non stocking locations' 
                        FROM I_InventoryPolicy di,
                        I_OpeningStocks ds
                        WHERE di.ItemId = ds.ItemId
                        and   di.LocationId = ds.LocationId
                        and   di.IsStorage = 0
                        and   ds.Quantity > 0"""

null_supply_capacity_query = """select 'I_ResourceMaster', 'ResourceId', ResourceId, 'Warning', 'Supply Capacity is empty'
                                from I_ResourceMaster
                                where SupplyCapacity is null"""

no_manufacturing_warning = """SELECT  'I_Processes', 
                                        '(ItemId, LocationId)',
                                        '('||dop.ItemId||','||dop.LocationId||')',
                                        'Warning',
                                        'This record has no make record in I_InventoryPolicy table'
                                FROM I_Processes dop,
                                I_InventoryPolicy di
                                WHERE dop.ItemId = di.ItemId
                                AND   dop.LocationId = di.LocationId
                                AND   di.IsProduction = 0"""

inactive_items = """select 'I_ItemMaster', 'ItemId', ItemId, 'Warning', 'Inactive Item'
                    from I_ItemMaster
                    WHERE ItemStatus != 1"""

unit_cost_sales_price_check = """select 'I_ItemMaster', 'ItemId', ItemId, 'Warning', 'UnitCost is greater than sales price'
                                from I_ItemMaster
                                WHERE UnitCost is not null
                                AND   SalesPrice is not null
                                AND   UnitCost > SalesPrice"""

