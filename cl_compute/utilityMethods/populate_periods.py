from ..sql_connector import insert_log

def main(conn):
    '''Populate periods in O_Period table based on I_ModelSetup table '''
    conn.execute("DELETE FROM O_Period")
    query = """INSERT INTO O_Period (PeriodIdx, PeriodStart, PeriodEnd)
                    with recursive cnt(x)
                    AS
                        ( 
                        Values(0)
                        UNION ALL
                        select x + 1
                        from cnt,
                            I_ModelSetup
                        where x < I_ModelSetup.NumberOfPeriods - 1
                        )
                    select x, CASE WHEN TimeFrequency = 'Daily'
                        THEN date(I_ModelSetup.StartDate, '+'||x||' days')
                        WHEN TimeFrequency = 'Weekly'
                        THEN date(I_ModelSetup.StartDate, '+'||x * 7||' days')
                        WHEN TimeFrequency = 'Monthly'
                        THEN date(I_ModelSetup.StartDate, '+'||x||' months')
                        WHEN TimeFrequency = 'Quarterly'
                        THEN date(I_ModelSetup.StartDate, '+'||x * 3||' months')
                        WHEN TimeFrequency = 'Yearly'
                        THEN date(I_ModelSetup.StartDate, '+'||x * 12||' months')
                        end as periodStartDate,
                        CASE WHEN TimeFrequency = 'Daily'
                        THEN date(I_ModelSetup.StartDate, '+'||x+1||' days')
                        WHEN TimeFrequency = 'Weekly'
                        THEN date(I_ModelSetup.StartDate, '+'||(x+1) * 7||' days')
                        WHEN TimeFrequency = 'Monthly'
                        THEN date(I_ModelSetup.StartDate, '+'||(x+1)||' months')
                        WHEN TimeFrequency = 'Quarterly'
                        THEN date(I_ModelSetup.StartDate, '+'||(x+1) * 3||' months')
                        WHEN TimeFrequency = 'Yearly'
                        THEN date(I_ModelSetup.StartDate, '+'||(x+1) * 12||' months')
                        end as periodEndDate
                    from cnt, I_ModelSetup"""
    conn.execute(query)
    update_query = """UPDATE O_Period
                        set PeriodDays = julianday(PeriodEnd) - julianday(PeriodStart),
                            PeriodYear = substr(PeriodStart,1,4),
                            PeriodMonth = substr(PeriodStart,1,7),
                            PeriodQuarter = substr(PeriodStart,1,4) || '_' ||
                                            CASE WHEN  substr(PeriodStart,6,2) IN ('01', '02', '03') THEN 'Q1'
                                            WHEN substr(PeriodStart,6,2) IN ('04', '05', '06') THEN 'Q2'
                                            WHEN substr(PeriodStart,6,2) IN ('07', '08', '09') THEN 'Q3'
                                            WHEN substr(PeriodStart,6,2) IN ('10', '11', '12') THEN 'Q4'
                                            END"""
    conn.execute(update_query)
    conn.intermediate_commit()