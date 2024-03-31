{% set suffix = '_mtmp' %}
declare
    @new_table_name varchar (100) = '{{ name  + suffix  }}',
    @old_table_name varchar(100) = '{{ name}}',
    @table_owner varchar (100) = '{{ local_context_dict.dbschema }}',
    @table_qualifier varchar (100) = '{{ local_context_dict.basedatabase }}'

BEGIN
-- SET NOCOUNT ON added to prevent extra result sets from
-- interfering with SELECT statements.
SET NOCOUNT ON;

BEGIN TRANSACTION;

    DECLARE 
        @SQL varchar(200),
        @pkey_name varchar (100),
        @tmp_column_name varchar (150),
        @tmp_pk_name varchar (150);


    -- COPY STRUCTURE AND DATAS (except keys, constraint... etc)
    SET @SQL = 'SELECT * INTO ' + @table_owner + '.' + @new_table_name + ' FROM ' + @table_owner + '.' + @old_table_name + ' WHERE 1 = 0';
    EXEC (@SQL);


    -- PRIMARY KEYS TABLE
    DECLARE @table_primary_keys TABLE (
        TABLE_QULIFER varchar(150),
        TABLE_OWNER varchar(150),
        TABLE_NAME varchar(150),
        COLUMN_NAME varchar(150),
        KEY_SEQ INT,
        PK_NAME varchar(150)
    )

    INSERT INTO @table_primary_keys EXEC sp_pkeys @old_table_name, @table_owner, @table_qualifier;

    -- Contrainst name
    SELECT @pkey_name = PK_NAME FROM @table_primary_keys GROUP BY PK_NAME;

    DECLARE cursor_primary_key CURSOR FOR
        SELECT COLUMN_NAME, PK_NAME FROM @table_primary_keys;

    OPEN cursor_primary_key;
    FETCH NEXT FROM cursor_primary_key INTO @tmp_column_name, @tmp_pk_name;

    SET @SQL = 'ALTER TABLE ' + @table_owner + '.' + @new_table_name + ' ADD CONSTRAINT pk_' + @new_table_name + ' PRIMARY KEY CLUSTERED ('; 
    WHILE @@FETCH_STATUS = 0
    BEGIN 
        IF @pkey_name <> @tmp_pk_name
        BEGIN;
            THROW 50000, 'Two primary keys differents.', 1;
        END;
        SET @SQL = @SQL + @tmp_column_name + ', ';

        FETCH NEXT FROM cursor_primary_key INTO @tmp_column_name, @tmp_pk_name;
    END
    SET @SQL = SUBSTRING(@SQL, 1, LEN(@SQL) - 1) + ')';

    EXEC (@SQL);

COMMIT TRANSACTION;
END