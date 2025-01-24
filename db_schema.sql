BEGIN TRANSACTION;

---- CL TABLES Start
CREATE TABLE T_QueryLogs (
	LogTime      VARCHAR DEFAULT (datetime('now', 'localtime') ),
	QuerySQL     VARCHAR,
	QueryMsg     VARCHAR
);

CREATE TABLE T_TaskLogs (
        ID              INTEGER PRIMARY KEY AUTOINCREMENT,
        TaskId          VARCHAR UNIQUE,
        TaskName        VARCHAR NOT NULL,
        ProcessId       VARCHAR,
        TaskStatus      VARCHAR,
        StartDate       VARCHAR DEFAULT (datetime('now', 'localtime') ),
        EndDate         VARCHAR,
        ErrorMsg        VARCHAR,
        Alerted         INTEGER DEFAULT (0),
        TaskDbId        VARCHAR,
        MasterTaskId    VARCHAR
);

CREATE TABLE T_SolverLog (
    LogTime    VARCHAR DEFAULT (datetime('now', 'localtime') ),
    LogMessage VARCHAR
);

CREATE TABLE S_ModelParams (
    ParamName    VARCHAR,
    ParamValue   VARCHAR
);

CREATE TABLE S_TableParameters (
    TableName      VARCHAR,
    ColumnName     VARCHAR,
    ParameterType  VARCHAR,
    ParameterValue VARCHAR,
    UNIQUE(TableName,ColumnName,ParameterType)
);

CREATE TABLE S_TableGroup (
    GroupName        VARCHAR,
    TableName        VARCHAR,
    TableDisplayName VARCHAR,
    TableType        VARCHAR,
    ColumnOrder      VARCHAR,
    Table_Status     VARCHAR,
    Freeze_Col_Num   NUMERIC
);

CREATE TABLE S_TaskMaster (
	TaskId	            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	TaskName	        VARCHAR,
	TaskDisplayName	    VARCHAR,
	TaskParameters	    VARCHAR,
	TaskStatus	        VARCHAR,
	TaskLastRunDate	    VARCHAR,
	TaskOutput	        VARCHAR
);

CREATE TABLE S_ExecutionFiles (
	FileId	            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	FileName	        VARCHAR,
	FileLabel   	    VARCHAR,
    FilePath            VARCHAR UNIQUE,
    FileData            VARCHAR,
	Status	            VARCHAR DEFAULT ('Active')	
);

CREATE TABLE S_DataFiles (
	FileId	            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	FileName	        VARCHAR,
	FileType   	        VARCHAR,
    FileBlob            BLOB NOT NULL,
	Status	            VARCHAR DEFAULT ('Active'),
    UNIQUE(FileName,FileType)	
);

CREATE TABLE S_PackageWheels (
	WheelId	            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	WheelName	        VARCHAR UNIQUE,
    WheelBlob            BLOB NOT NULL,
	Status	            VARCHAR DEFAULT ('Active')	
);

CREATE VIEW V_TEMPV
AS SELECT 1;


INSERT INTO S_ModelParams (ParamName, ParamValue) VALUES ('ModelIcon', 'fas fa-cube');
INSERT INTO S_ModelParams (ParamName, ParamValue) VALUES ('ModelName', 'Supply Planning DB');
INSERT INTO S_ModelParams (ParamName, ParamValue) VALUES ('DBVersion', '1.0.3');

INSERT INTO S_ExecutionFiles VALUES(1,'requirements.txt',NULL,'requirements.txt',replace('urllib3\nPillow\npandas\nsqlite3\npulp\nhighspy','\n',char(10)),'Active');

---- CL TABLES End

-- Table: I_ItemMaster
CREATE TABLE I_ItemMaster (
    ItemId                VARCHAR,
    ItemDescription       VARCHAR,
    ItemType              VARCHAR,
    SalesPrice            NUMERIC,
    UnitCost              NUMERIC,
    Brand                 VARCHAR,
    SubBrand              VARCHAR,
    Category              VARCHAR,
    SubCategory           VARCHAR,
    ItemStatus            INTEGER DEFAULT 1 
);

-- Table: I_LocationMaster
CREATE TABLE I_LocationMaster (
    LocationId          VARCHAR,
    LocationType        VARCHAR,
    LocationCategory    VARCHAR,
    LocationAttribute1  VARCHAR,
    LocationAttribute2  VARCHAR,
    Region              VARCHAR,
    Country             VARCHAR,
    State               VARCHAR,
    City                VARCHAR,
    ZipCode             VARCHAR,
    Longitude           NUMERIC,
    Latitude            NUMERIC
);

-- Table: I_ModelSetup
CREATE TABLE I_ModelSetup (
    ModelName        VARCHAR,
    StartDate        VARCHAR,
    TimeFrequency    VARCHAR,
    NumberOfPeriods  NUMERIC,
    InterestRate     NUMERIC DEFAULT (0.12),
    DOSWindowStartPeriod INTEGER DEFAULT (1) 
);

-- Table: I_BOMRecipe
CREATE TABLE I_BOMRecipe (
    BOMId           VARCHAR,
    ItemId          VARCHAR,
    LocationId      VARCHAR,
    UsageQuantity   NUMERIC
);

-- Table: I_ForecastOrders
CREATE TABLE I_ForecastOrders (
    OrderId             VARCHAR,
    ItemId              VARCHAR,
    LocationId          VARCHAR,
    ForecastArrivalDate VARCHAR,
    Quantity            NUMERIC,
    SalesPrice          NUMERIC
);

-- Table: I_InventoryPolicy
CREATE TABLE I_InventoryPolicy (
    ItemId                      VARCHAR,
    LocationId                  VARCHAR,
    InventoryType               VARCHAR,
    IsProduction                INTEGER DEFAULT 0,
    IsStorage                   INTEGER DEFAULT 0,
    InventoryUnitCost           NUMERIC,
    InventoryHoldingCost        NUMERIC,
    SalesPrice                  NUMERIC,
    SafetyStockDOS              NUMERIC DEFAULT (0),
    DOSWindow                   NUMERIC DEFAULT (0),
    InventoryShelfLife          NUMERIC,
    MinReleaseTime              NUMERIC,
    MinEndingInventory          NUMERIC DEFAULT (0),
    MaxEndingInventory          VARCHAR DEFAULT ('INF'),
    MinProductionQuantity       NUMERIC DEFAULT (0),
    MaxProductionQuantity       VARCHAR DEFAULT ('INF'),
    InventoryStatus             INTEGER DEFAULT (1)
);

-- Table: I_InventoryPolicyPerPeriod
CREATE TABLE I_InventoryPolicyPerPeriod (
    ItemId                VARCHAR,
    LocationId            VARCHAR,
    StartDate             VARCHAR,
    MinEndingInventory    NUMERIC DEFAULT (0),
    MaxEndingInventory    VARCHAR DEFAULT ('INF'),
    MinProductionQuantity NUMERIC DEFAULT (0),
    MaxProductionQuantity VARCHAR DEFAULT ('INF'),
    SafetyStockDOS        NUMERIC
);

-- Table: I_Processes
CREATE TABLE I_Processes (
    ProcessId          VARCHAR,
    ProcessStep        VARCHAR,
    ItemId             VARCHAR,
    LocationId         VARCHAR,
    BOMId              VARCHAR,
    ResourceId         VARCHAR,
    UnitOperationTime  NUMERIC,
    UnitOperationCost  NUMERIC,
    Yield              NUMERIC DEFAULT 1,
    MOQ                NUMERIC,
    PersonnelTime      NUMERIC,
    LotSizeRounding    NUMERIC,
    MinSplitRatio      NUMERIC,
    MaxSplitRatio      NUMERIC
);

-- Table: I_ProcessesPerPeriod
CREATE TABLE I_ProcessesPerPeriod (
    ProcessId          VARCHAR,
    ProcessStep        VARCHAR,
    ItemId             VARCHAR,
    LocationId         VARCHAR,
    StartDate          VARCHAR,
    UnitOperationTime  NUMERIC,
    Yield              NUMERIC DEFAULT 1,
    MinSplitRatio      NUMERIC,
    MaxSplitRatio      NUMERIC
);

-- Table: I_ResourceMaster
CREATE TABLE I_ResourceMaster (
    ResourceId            VARCHAR,
    ResourceDescription   VARCHAR,
    LocationId            VARCHAR,
    ResourceUOM           VARCHAR,
    SupplyCapacity        VARCHAR DEFAULT ('INF'),
    MinUtilization        NUMERIC DEFAULT (0),
    MaxUtilization        VARCHAR DEFAULT ('1'),
    ResourceStatus        INTEGER DEFAULT (1)
);

-- Table: I_ResourcePerPeriod
CREATE TABLE I_ResourcePerPeriod (
    ResourceId     VARCHAR,
    StartDate      VARCHAR,
    SupplyCapacity VARCHAR,
    MinUtilization NUMERIC,
    MaxUtilization VARCHAR
);

-- Table: I_OpeningStocks
CREATE TABLE I_OpeningStocks (
    StockId      VARCHAR,
    ItemId       VARCHAR,
    LocationId   VARCHAR,
    Quantity     NUMERIC,
    EntryDate    VARCHAR,
    ExpiryDate   VARCHAR DEFAULT (72686) 
);

-- Table: I_TransportationPolicy
CREATE TABLE I_TransportationPolicy (
    ItemId                  VARCHAR,
    FromLocationId          VARCHAR,
    ToLocationId            VARCHAR,
    ModeId                  VARCHAR,
    UnitTransportationCost  NUMERIC,
    TransportationLeadTime  NUMERIC,
    MinQuantity             NUMERIC,
    MaxQuantity             NUMERIC,
    MinSplitRatio           NUMERIC,
    MaxSplitRatio           NUMERIC 
);

-- Table: I_TransportationPolicyPerPeriod
CREATE TABLE I_TransportationPolicyPerPeriod (
    ItemId                  VARCHAR,
    FromLocationId          VARCHAR,
    ToLocationId            VARCHAR,
    ModeId                  VARCHAR,
    StartDate               VARCHAR,
    MinQuantity             NUMERIC DEFAULT (0),
    MaxQuantity             VARCHAR DEFAULT ('INF'),
    MinSplitRatio           NUMERIC,
    MaxSplitRatio           NUMERIC 
);

-- Table: I_ForecastRegistration
CREATE TABLE I_ForecastRegistration (
    ForecastItemId       VARCHAR,
    ItemId               VARCHAR,
    LocationId		     VARCHAR,
    StartDate			 VARCHAR,
    EndDate			     VARCHAR
);

CREATE TABLE O_ModelValidation (
    TableName       VARCHAR,
    ColumnName     VARCHAR,
    ColumnValue     VARCHAR,
    ErrorType       VARCHAR,
    ErrorMsg        VARCHAR
);

CREATE TABLE O_Transportation (
    ItemId                  VARCHAR,
    FromLocationId          VARCHAR,
    ToLocationId            VARCHAR,
    StartDate               VARCHAR,
    EndDate                 VARCHAR,
    ModeId                  VARCHAR,
    FlowQuantity            NUMERIC,
    FlowTransportationCost  NUMERIC
);

CREATE TABLE O_Inventory (
    ItemId                  VARCHAR,
    LocationId              VARCHAR,
    StartDate               VARCHAR,
    OpeningInventory        NUMERIC,
    IncomingStock           NUMERIC,
    EndingInventory         NUMERIC,
    InTransitInventory      NUMERIC,
    InReleaseInventory      NUMERIC,
    ShortFallInventory      NUMERIC,
    InboundStock            NUMERIC,
    OutboundStock           NUMERIC,
    ProductionQuantity      NUMERIC,
    OrderedQuantity         NUMERIC,
    ConsumedQuantity        NUMERIC,
    ExpiredQuanity          NUMERIC,
    SatisfiedDemand         NUMERIC,
    Demand                  NUMERIC,
    RegistrationOutbound    NUMERIC,
    RegistrationInbound     NUMERIC,
    RequiredInventory       NUMERIC
);

CREATE TABLE O_InitialInventory (
    ItemId                  VARCHAR,
    LocationId              VARCHAR,
    Quantity                NUMERIC
);

CREATE TABLE O_Production (
    ItemId                 VARCHAR,
    LocationId             VARCHAR,
    ProcessId              VARCHAR,
    StartDate              VARCHAR,
    ProductionQuantity     NUMERIC,
    ProductionCost         NUMERIC
);

CREATE TABLE O_ForecastRegistration (
    ItemId                 VARCHAR,
    LocationId             VARCHAR,
    StartDate              VARCHAR,
    ForecastItemId         VARCHAR,
    SatisfiedQuantity      NUMERIC
);

CREATE TABLE O_Objective (
    SolveStatus            VARCHAR,
    ObjectiveName          VARCHAR,
    ObjectiveValue         NUMERIC,
    LastUpdateDate         VARCHAR DEFAULT (datetime('now', 'localtime') )
);

CREATE TABLE O_DemandAnalysis (
    ItemId                 VARCHAR,
    LocationId             VARCHAR,
    FulFilledQuantity      NUMERIC,
    Quantity               NUMERIC,
    Iteration              INTEGER
);

CREATE TABLE O_Period (
    PeriodIdx              INTEGER,
    PeriodStart            VARCHAR,
    PeriodEnd              VARCHAR,
    PeriodMonth            VARCHAR,
    PeriodQuarter          VARCHAR,
    PeriodYear             VARCHAR,
    PeriodDays             NUMERIC
);

INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_ModelSetup', 'StartDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_ForecastOrders', 'ForecastArrivalDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_OpeningStocks', 'EntryDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_OpeningStocks', 'ExpiryDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_ForecastRegistration', 'StartDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_ForecastRegistration', 'EndDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_InventoryPolicyPerPeriod', 'StartDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_ProcessesPerPeriod', 'StartDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_ResourcePerPeriod', 'StartDate', 'LOV', 'Date');
INSERT INTO S_TableParameters (TableName, ColumnName, ParameterType, ParameterValue) VALUES ('I_TransportationPolicyPerPeriod', 'StartDate', 'LOV', 'Date');


INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_ItemMaster', 'Items', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_LocationMaster', 'Locations', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_InventoryPolicy', 'Inventory Policy', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_ModelSetup', 'Model Setup', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_ForecastOrders', 'Forecasts', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_Processes', 'Operation Processes', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_BOMRecipe', 'Bill Of Materials', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_ResourceMaster', 'Resource', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_OpeningStocks', 'Opening Stocks', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_TransportationPolicy', 'Transportation', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Input Tables', 'I_ForecastRegistration', 'Registration Calendar', 'Input', NULL, 'Active', NULL);

INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Multiperiod Tables', 'I_InventoryPolicyPerPeriod', 'Inv Policy - Period', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Multiperiod Tables', 'I_ProcessesPerPeriod', 'Operation Processes - Period', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Multiperiod Tables', 'I_TransportationPolicyPerPeriod', 'Transportation Policy - Period', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Multiperiod Tables', 'I_ResourcePerPeriod', 'Resource - Period', 'Input', NULL, 'Active', NULL);


INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Output Tables', 'O_ModelValidation', 'Verify Logs', 'Output', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Output Tables', 'O_Period', 'Periods', 'Output', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Output Tables', 'O_Inventory', 'Inventory Ouput', 'Output', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Output Tables', 'O_Transportation', 'Transportation Output', 'Output', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Output Tables', 'O_Production', 'Production Output', 'Output', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Output Tables', 'O_Objective', 'Objective Output', 'Output', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Output Tables', 'O_ForecastRegistration', 'Forecast Registration', 'Output', NULL, 'Active', NULL);

INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Log Tables', 'T_TaskLogs', 'Task Logs', 'Task Logs', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Log Tables', 'T_SolverLog', 'Solver Logs', 'Solver Logs', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Setups', 'S_TableGroup', 'Table Group', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Setups', 'S_TableParameters', 'Table Parameters', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Setups', 'S_TaskMaster', 'Task Master', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('Setups', 'S_ExecutionFiles', 'Code Files', 'Input', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES ('All Other', 'V_TEMPV', 'Temp View', 'Output', NULL, 'Active', NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES('All Other','S_DataFiles','Data Files','Input','["FileId","FileName","FileType","Status"]','Active',NULL);
INSERT INTO S_TableGroup (GroupName, TableName, TableDisplayName, TableType, ColumnOrder, Table_Status, Freeze_Col_Num) VALUES('All Other','S_PackageWheels','PackageWheels','Input','["WheelId","WheelName","Status"]','Active',NULL);

COMMIT TRANSACTION;