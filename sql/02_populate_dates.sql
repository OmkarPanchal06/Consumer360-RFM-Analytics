USE Consumer360_DB;
GO

DECLARE @StartDate DATE = '2020-01-01';
DECLARE @EndDate DATE = '2030-12-31';
DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
BEGIN
    INSERT INTO Dim_Date (
        DateKey,
        CalendarDate,
        Year,
        Quarter,
        Month,
        Day,
        WeekOfYear,
        DayOfWeek,
        DayName,
        MonthName,
        IsWeekend,
        IsHoliday
    )
    VALUES (
        CAST(FORMAT(@CurrentDate, 'yyyyMMdd') AS INT),
        @CurrentDate,
        YEAR(@CurrentDate),
        DATEPART(QUARTER, @CurrentDate),
        MONTH(@CurrentDate),
        DAY(@CurrentDate),
        DATEPART(WEEK, @CurrentDate),
        DATEPART(WEEKDAY, @CurrentDate),
        FORMAT(@CurrentDate, 'dddd'),
        FORMAT(@CurrentDate, 'MMMM'),
        CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1 ELSE 0 END,
        0
    );
    
    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END;

PRINT '✓ Date dimension populated successfully!';