package main

import (
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	*sql.DB
}

const (
	SQL_CREATE_FUNCTION = `
		CREATE FUNCTION $table-name_func()
		RETURNS TRIGGER
		AS $$
		BEGIN
			IF TG_OP = 'INSERT' THEN
				INSERT INTO $table-name_log VALUES
					(NEW.*, TG_OP);
				RETURN NEW;
			ELSIF TG_OP = 'UPDATE' THEN
				DELETE FROM $table-name_log WHERE id = NEW.id;
				INSERT INTO $table-name_log VALUES
						( NEW.*, TG_OP);
				RETURN NEW;
			ELSIF TG_OP = 'DELETE' THEN
				INSERT INTO $table-name_log VALUES
						(OLD.*, TG_OP);
				RETURN OLD;
			END IF;
		END;
		$$
		LANGUAGE plpgsql;
	`
	SQL_ADD_COLUMN = `
	DO $$ 
    BEGIN
        BEGIN
            ALTER TABLE %s ADD COLUMN %s %s;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column %s already exists in %s.';
        END;
    END;
	$$
	`

	SQL_CREATE_TRIGGER = `
		CREATE TRIGGER $table-name_trigger
		AFTER INSERT OR UPDATE OR DELETE ON $table-name
		FOR EACH ROW EXECUTE PROCEDURE $table-name_func();
	`
	SQL_COPPY_TABLE_FUNCTION = `
		CREATE OR REPLACE FUNCTION create_table_like(_tbl regclass, _newtbl text)
			RETURNS void AS
				$func$
				DECLARE
				_sql text;
				BEGIN
				EXECUTE format('SELECT create_sequence_func(''' || _newtbl || ''',''' || quote_ident(_tbl||'') || ''');');
				EXECUTE format('DROP TABLE IF EXISTS %I;', _newtbl);
				-- Copy table
				EXECUTE format('CREATE TABLE %I (LIKE %s INCLUDING defaults EXCLUDING STORAGE);', _newtbl, _tbl);

				-- Fix serial columns, if any    
				SELECT INTO _sql
						string_agg(format('ALTER SEQUENCE %s OWNED BY %I.%I'
										, seq, _newtbl, a.attname), E';\n') || E';\n'
					|| 'ALTER TABLE ' || quote_ident(_newtbl) || E'\n  '
					|| string_agg(format($$ALTER %I SET DEFAULT nextval('%s'::regclass)$$
												, a.attname, seq), E'\n, ')
				FROM   pg_attribute  a
				JOIN   pg_attrdef    ad ON ad.adrelid = a.attrelid
									AND ad.adnum   = a.attnum
					, quote_ident(_tbl || '_' || a.attname || '_seq') AS seq 
				WHERE  a.attrelid = _tbl
				AND    a.attnum > 0
				AND    NOT a.attisdropped
				AND    a.atttypid = ANY ('{int,int8,int2}'::regtype[])
				AND    ad.adsrc = 'nextval('''
						|| (pg_get_serial_sequence (a.attrelid::regclass::text, a.attname))::regclass
						|| '''::regclass)';
				IF _sql IS NOT NULL THEN
				EXECUTE _sql;
				END IF;
				END
			$func$  LANGUAGE plpgsql VOLATILE;
	`

	SQL_SEQUENCE_FUNC = `
	CREATE OR REPLACE FUNCTION create_sequence_func( _tbl text, _newtbl text)
			RETURNS void AS
				$func$
				DECLARE
				_sql text;
				BEGIN
                  IF EXISTS(SELECT *
                   FROM information_schema.tables
                   WHERE table_schema = current_schema()
                         AND table_name = quote_ident(_tbl||'')) THEN
				-- Fix serial columns, if any    
				    SELECT INTO _sql
						string_agg(format('ALTER SEQUENCE %s OWNED BY %I.%I'
										, seq, _newtbl, a.attname), E';\n') || E';\n'
					|| 'ALTER TABLE ' || quote_ident(_newtbl) || E'\n  '
					|| string_agg(format($$ALTER %I SET DEFAULT nextval('%s'::regclass)$$
												, a.attname, seq), E'\n, ')
					FROM   pg_attribute  a
					JOIN   pg_attrdef    ad ON ad.adrelid = a.attrelid
										AND ad.adnum   = a.attnum
						, quote_ident(_newtbl || '_' || a.attname || '_seq') AS seq 
					WHERE  a.attrelid = _tbl::regclass
					AND    a.attnum > 0
					AND    NOT a.attisdropped
					AND    a.atttypid = ANY ('{int,int8,int2}'::regtype[])
					AND    ad.adsrc = 'nextval('''
							|| (pg_get_serial_sequence (a.attrelid::regclass::text, a.attname))::regclass
							|| '''::regclass)';
					IF _sql IS NOT NULL THEN
						EXECUTE _sql;
					END IF;
                END IF;
				END
		$func$  LANGUAGE plpgsql VOLATILE;
	`

	SQL_SWAP_RECORD = `
		INSERT INTO %s
			SELECT * FROM %s
	`
	SQL_RENAME_TABLE = `
		BEGIN;
			ALTER TABLE %s
				RENAME TO %s;
			ALTER TABLE %s
				RENAME TO %s;
			%s
			%s
			%s;
		COMMIT;
	`
	SQL_DROP_TABLE_IF_EXITS = `
		DROP TABLE IF EXISTS %s;
	`
	SQL_DROP_TRIGGER       = `DROP TRIGGER $table-name_trigger ON $table-name_temp_temp;`
	SQL_DROP_FUNC          = `DROP FUNCTION %s_func;`
	SQL_DROP_CREATE_FUNC   = `DROP FUNCTION create_table_like;`
	SQL_CONSTRAINTS_INDEXS = `SELECT indexdef, indexname FROM pg_indexes WHERE tablename = $1;`
	SQL_DROP_TEMP          = `DROP TABLE %s;`
	SQL_RENAME_INDEX       = "ALTER INDEX %s RENAME TO %s"
)

func main() {
	dbHost := flag.String("h", "127.0.0.1:5432", "Database host")
	username := flag.String("u", "apple", "Database username")
	password := flag.String("p", "", "Database password")
	dbName := flag.String("d", "apple", "Database name")
	tableName := flag.String("t", "", "Database name table")
	action := flag.String("a", "NEW", "action CLEAN, NEW default NEW")
	flag.Parse()
	start := time.Now()
	fmt.Printf("Connecting to database %s ....\n", *dbName)
	db, err := connectPostgresqlDatabase(*dbHost, *username, *password, *dbName)
	if err != nil {
		fmt.Println("Error connect DB: ", err.Error())
	}
	elapsed := time.Since(start)
	fmt.Printf("----->  run with time: %s\n", elapsed)
	if strings.ToUpper(*action) == "CLEAN" {
		db.cleanOriginTable(*tableName)
	} else if strings.ToUpper(*action) == "NEW" {
		db.stepCreateTableLikeFuncAndSequencsFunc()
		// step create table log
		db.stepCreateTableLog(*tableName)
		// step create trigger
		db.stepCreateTriggerAction(*tableName)
		// step swap record
		indexQuery, _ := db.swapDataInTable(*tableName)
		// step apply from log to new table
		db.applyRecordFromLogToCurrentTable(*tableName)
		db.execIndex(indexQuery)
	}
}

func (db *DB) execIndex(indexQuery string) error {
	_, err := db.Exec(indexQuery)
	if err != nil {
		fmt.Println("Error exec index: ", err.Error())
	}
	return err
}

func (db *DB) stepCreateTableLikeFuncAndSequencsFunc() error {
	_, err := db.Exec(SQL_COPPY_TABLE_FUNCTION)
	if err != nil {
		fmt.Println("Error exec function coppy: ", err.Error())
	}
	_, err = db.Exec(SQL_SEQUENCE_FUNC)
	if err != nil {
		fmt.Println("Error exec function sequence: ", err.Error())
	}
	return err
}

func (db *DB) stepCreateTableLog(tableName string) error {
	tableLog := fmt.Sprintf("%s_log", tableName)
	fmt.Printf("====== Begin create Table %s_log ======\n", tableName)
	start := time.Now()
	_, err := db.Exec(fmt.Sprintf(`%s CREATE TABLE %s (LIKE %s INCLUDING defaults EXCLUDING STORAGE);`, fmt.Sprintf(SQL_DROP_TABLE_IF_EXITS, tableLog), tableLog, tableName))
	if err != nil {
		fmt.Println("Error create log: ", err)
		return err
	}
	_, err = db.Exec(fmt.Sprintf(SQL_ADD_COLUMN, tableLog, "action", "varchar(200)", "action", tableLog))
	if err != nil {
		fmt.Println("Error add column log: ", err.Error())
		return err
	}
	elapsed := time.Since(start)
	fmt.Printf("----->  run with time: %s\n", elapsed)
	return err
}

func (db *DB) stepCreateTriggerAction(tableName string) error {
	db.createFunctionInsertToLog(tableName)
	fmt.Printf("====== Begin create trigger %s_trigger ======\n", tableName)
	start := time.Now()
	_, err := db.Exec(fmt.Sprintf(strings.ReplaceAll(SQL_CREATE_TRIGGER, "$table-name", tableName)))
	if err != nil {
		fmt.Println("Error create trigger: ", err.Error())
		return err
	}
	elapsed := time.Since(start)
	fmt.Printf("----->  run with time: %s\n", elapsed)
	return err
}

func (db *DB) createFunctionInsertToLog(tableName string) error {
	fmt.Printf("====== Begin create function %s_func ======\n", tableName)
	start := time.Now()
	_, err := db.Exec(fmt.Sprintf(strings.ReplaceAll(SQL_CREATE_FUNCTION, "$table-name", tableName)))
	if err != nil {
		fmt.Println("Error create function: ", err.Error())
		return err
	}
	elapsed := time.Since(start)
	fmt.Printf("----->  run with time: %s\n", elapsed)
	return err
}

func (db *DB) swapDataInTable(tableName string) (string, error) {
	fmt.Printf("====== Begin swap record to %s_temp ======\n", tableName)
	start := time.Now()
	tableTemp := fmt.Sprintf("%s_temp", tableName)
	_, err := db.Exec(fmt.Sprintf(`SELECT create_table_like('%s','%s')`, tableName, tableTemp))
	if err != nil {
		fmt.Println("Error create temp table: ", err.Error())
		return "", err
	}
	_, err = db.Exec(fmt.Sprintf(SQL_SWAP_RECORD, tableTemp, tableName))
	if err != nil {
		fmt.Println("Error swap record: ", err.Error())
		return "", err
	}

	fmt.Println("* rename table a -> c b -> a \n")
	tableTempTemp := fmt.Sprintf("%s_temp_temp", tableName)

	fmt.Println("==> Find constsaints..")
	var sqlConstraints = ""
	var sqlRenameIndex = ""
	indexdefs, indexnames := db.findAllConstraints(tableName, tableTempTemp)
	if len(indexdefs) > 0 {
		sqlConstraints = strings.Join(indexdefs, ";")
	}
	if len(indexnames) > 0 {
		sqlRenameIndex = strings.Join(indexnames, ";")
	}
	dropTriggerSQl := strings.ReplaceAll(SQL_DROP_TRIGGER, "$table-name", tableName)
	dropFuncSQL := fmt.Sprintf(SQL_DROP_FUNC, tableName)
	_, err = db.Exec(fmt.Sprintf(SQL_RENAME_TABLE, tableName, tableTempTemp, tableTemp, tableName, dropTriggerSQl, dropFuncSQL, sqlRenameIndex))
	if err != nil {
		fmt.Println("Error rename table a -> c b -> a ", err.Error())
		return "", err
	}
	elapsed := time.Since(start)
	fmt.Printf("----->  run with time: %s\n", elapsed)
	return sqlConstraints, err
}

func (db *DB) cleanOriginTable(tableName string) error {
	fmt.Printf("====== Begin drop %s_temp ======\n", tableName)
	start := time.Now()
	tableTempTemp := fmt.Sprintf("%s_temp_temp", tableName)
	dropTableTemp := fmt.Sprintf(SQL_DROP_TEMP, tableTempTemp)
	_, err := db.Exec(dropTableTemp)
	if err != nil {
		fmt.Println("Drop table temp origin", err.Error())
		return err
	}
	tableLog := fmt.Sprintf("%s_log", tableName)
	dropTablelog := fmt.Sprintf(SQL_DROP_TEMP, tableLog)
	_, err = db.Exec(dropTablelog)
	if err != nil {
		fmt.Println("Drop table log origin", err.Error())
		return err
	}
	elapsed := time.Since(start)
	fmt.Printf("----->  run with time: %s\n", elapsed)
	return nil
}

func (db *DB) applyRecordFromLogToCurrentTable(tableName string) error {
	tx, err := db.Begin()
	if err != nil {
		fmt.Println("Error begin -----")
	}
	tableLog := fmt.Sprintf("%s_log", tableName)
	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s`, tableLog))
	if err != nil {
		fmt.Println("Error apply log table: ", err.Error())
		return err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	for rows.Next() {
		columns := make([]string, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}
		err := rows.Scan(columnPointers...)
		if err != nil {
			tx.Rollback()
			fmt.Println(err.Error())
			break
		}
		var errNomal error
		columnsSlice := columnPointers[:len(columnPointers)-1]
		switch string(columns[len(cols)-1]) {
		case "DELETE":
			sqlDelete := fmt.Sprintf(`
			DELETE FROM %s WHERE %s=$1;
			`, tableName, cols[0])
			_, err := db.Exec(sqlDelete, columnsSlice[0])
			if err != nil {
				errNomal = err
			}
			break
		case "INSERT":
			insertFields := cols[:len(cols)-1]
			var valuseIndexs []string
			for i, _ := range insertFields {
				valuseIndexs = append(valuseIndexs, fmt.Sprintf("$%d", (i+1)))
			}
			sqlInsert := fmt.Sprintf(`
				INSERT INTO %s(%s)
				VALUES (%s)
				ON CONFLICT(%s)
				DO NOTHING
			`, tableName, strings.Join(insertFields, ","), strings.Join(valuseIndexs, ","), cols[0])
			fmt.Println(sqlInsert)
			_, err := db.Exec(sqlInsert, columnsSlice...)
			if err != nil {
				errNomal = err
			}
			break
		case "UPDATE":
			insertFields := cols[:len(cols)-1]
			var valuseIndexs []string
			for i, v := range insertFields {
				valuseIndexs = append(valuseIndexs, fmt.Sprintf("%s=$%d", v, (i+1)))
			}
			sqlUpdate := fmt.Sprintf(`
			UPDATE %s
			SET
				%s
			WHERE %s = $%d;
			`, tableName, strings.Join(valuseIndexs, ","), cols[0], len(cols))
			columnsSlice = append(columnsSlice, columnsSlice[0])
			_, err := db.Exec(sqlUpdate, columnsSlice...)
			if err != nil {
				errNomal = err
			}
			break
		}
		if errNomal != nil {
			fmt.Println(err.Error())
			tx.Rollback()
			break
		}
	}
	tx.Commit()
	return err
}

func (db *DB) findAllConstraints(tableName string, tableTempTemp string) ([]string, []string) {
	rows, err := db.Query(SQL_CONSTRAINTS_INDEXS, tableName)
	if err != nil {
		fmt.Println("\nError find constraints :", err.Error())
	}
	defer rows.Close()
	var indexdefs []string
	var indexnames []string
	for rows.Next() {
		var indexdef, indexname sql.NullString
		err = rows.Scan(&indexdef)
		if err != nil {
			fmt.Println("Error loop list constraints: ", err.Error())
		}
		indexdefs = append(indexdefs, indexdef.String)
		indexnames = append(indexnames, fmt.Sprintf(SQL_RENAME_INDEX, indexname.String, strings.ReplaceAll(indexname.String, tableName, tableTempTemp)))
	}
	return indexdefs, indexnames
}

func connectPostgresqlDatabase(dbHost, username, password, dbName string) (dbObject *DB, err error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		username,
		password,
		dbHost,
		dbName))
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(40)
	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}
