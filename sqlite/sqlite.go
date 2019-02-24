package sqlite

import (
	"errors"
	"io"
	"os"

	"github.com/mxk/go-sqlite/sqlite3"
)

var (
	closedErr = errors.New("SQLite database already closed")
)

// Перевод числового значения в логическое (в SQLite нет логического типа)
func Itob(val int64) bool {
	return val > 0
}

// .............................................................................

// RowMap - тип, обобщающий sqlite3.RowMap
type RowMap sqlite3.RowMap

// Проверяет, пуста ли карта значений
func (s RowMap) IsEmpty() bool {
	return len(s) == 0
}

// .............................................................................

// Структура объекта соединения с базой данных. Упрощает работу с базой данных в сравнении с sqlite3.Conn
type Conn struct {
	conn *sqlite3.Conn
}

// Проверка существования файла базы данных
func CheckBaseFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// Отктытие базы данных (если файл базы не существует, он будет создан)
func Open(basePath string) (res *Conn, err error) {
	var conn *sqlite3.Conn
	if conn, err = sqlite3.Open(basePath); err == nil {
		res = &Conn{conn: conn}
	}
	return
}

// Запрос, которому не ттребует получения данных из базы данных
func (s *Conn) Exec(query string, args ...interface{}) error {
	return s.conn.Exec(query, args...)
}

// Запрос, в результате которого наобходимо получить единственную запись из базы данных
func (s *Conn) Row(query string, args ...interface{}) (RowMap, error) {
	res := new(SQLResult)
	if res.handle, res.Err = s.conn.Query(query, args...); res.Err == nil {
		res.setup()
		res.Close()
		return res.Row, nil
	} else if res.Err == io.EOF {
		return res.Row, nil
	} else {
		return res.Row, res.Err
	}
}

// Запрос списка записей. SQLResult должен быть закрыт
func (s *Conn) Query(query string, args ...interface{}) *SQLResult {
	res := new(SQLResult)
	if res.handle, res.Err = s.conn.Query(query, args...); res.Err == nil {
		res.setup()
	}
	return res
}

// Созлание новой записи в базе данных. Возвращается rowid сделанной записи или ошибка
func (s *Conn) Insert(query string, args ...interface{}) (rowid int64, err error) {
	if err = s.Exec(query, args...); err == nil {
		rowid = s.conn.LastInsertId()
	}
	return
}

func (s *Conn) Begin() error        { return s.conn.Begin() }        // Старт транзакции
func (s *Conn) Commit() error       { return s.conn.Commit() }       // Подтверждение транзакции
func (s *Conn) Rollback() error     { return s.conn.Rollback() }     // Откат транзакции
func (s *Conn) LastInsertId() int64 { return s.conn.LastInsertId() } //

// Закрытие соединения с базой данных
func (s *Conn) Close() error {
	if s.conn != nil {
		if err := s.conn.Close(); err == nil {
			s.conn = nil
			return nil
		} else {
			return err
		}
	} else {
		return closedErr
	}
}

// ............................................................................

// Структура, объект которой возвращается при выборке с ненулевым результатом (одной записи или списка записей)
type SQLResult struct {
	handle *sqlite3.Stmt
	Row    RowMap
	Err    error
}

// Сканирует поле и устанавливает карту значений в объекте
func (s *SQLResult) setup() {
	row := make(sqlite3.RowMap)
	if s.Err = s.handle.Scan(row); s.Err == nil {
		s.Row = RowMap(row)
	}
}

// Сканирование следующей записи (при выборках списка записей)
func (s *SQLResult) Next() bool {
	if s.Err = s.handle.Next(); s.Err == nil {
		s.setup()
	}
	return s.Err == nil
}

// Закрытие объекта (объект обязательно должен быть закрыт после использования)
func (s *SQLResult) Close() {
	if s.handle != nil {
		s.handle.Close()
		s.handle = nil
	}
}
