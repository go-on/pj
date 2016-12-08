// Copyright (c) 2015 Marc René Arns. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/*
Package pj offers a very simple and thin opiniated layer over postgresql database queries.

Therefor it makes several assumptions.

1. Each query returns a single row and a single column that is the result of a calls of a
postgres function that gets one parameter that is a json string and returns a json string.
This is most easily achieved by using the plv8 extension to define the functions.

2. All validation occurs inside the postgresql function.

3. The parameter to the function is a json map created from the url query.

4. The returned json will be returned to the client.

5. The returned json may have a property "http_status_code" to indicate errors. If it does the corresponding status code is sent
to the client in addition to the json.

6. The returned json may have a property "http_headers" that must be convertible to a map[string]string. If it does, the http headers
will be set accordingly.

7. Authentication and authorization will be handled by middleware surrounding the http.Handler returned from pj.New

Benefits

- no mapping server<->database necessary for rows and tables

- single point of truth for structures/tables

- validation near the data

- easier schema migration: start with a function that returns static testdata and change the function to query
tables when the schema has settled

- fast development of client and database without having to restart or recompile server

Disadvantages

- you are bound to postgres

- need pg users for access roles

- need different connections for different access roles

- learn postgres

*/
package pj

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
)

// using hidden function in stdlib, see: https://github.com/golang/go/issues/18086
// copied to file json-validate
func isValidJSON(data []byte) error {
	var s scanner
	return checkValid(data, &s)
}

type Queryer interface {
	QueryRow(sql string, args ...interface{}) *sql.Row
}

type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type DB interface {
	QueryRow(sql string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// New creates a new http.Handler that dispatches to the given queries based on the
// queryMap. Queries are expected to be postgresql functions in PL/v8 that receive a stringified json object as input
// and return a stringified json object as output.
// These functions are expected to do validation etc. If such a function needs to return an error, the returned json object
// must have a property "http_status_code" that has the appropriate http status code that should be returned, as a number.
// then this status code will be returned alongside the json of the response.
// It is important to return an error status code, if an error occurs.
// If a result should be returned, a property "result" for a single row or "results" for multiple rows must be set.
// If a result has a "http_headers" property, this headers will be set
// the rest is a contract between the receiving javascript and the returning sql function
// The map maps request methods ("GET", "POST", "PUT", "PATCH", "DELETE") to postgres function names that are defined like above.
// If errTracker is not nil, all errors will be passed to it in addition to the normal error handling
func New(db Queryer, m map[string]string, errTracker func(error, *http.Request)) *PJ {
	for meth := range m {
		switch meth {
		case "GET", "POST", "PUT", "PATCH", "DELETE":
		default:
			panic("method " + meth + " is not allowed")
		}
	}

	return &PJ{m, db, errTracker, 2048}
}

type PJ struct {
	Map         map[string]string
	Queryer     Queryer
	errTracker  func(error, *http.Request)
	MaxBodySize int64 // max size of the body, defaults to 2KB
}

func (p *PJ) getRow(r *http.Request) (*sql.Row, error) {
	if r.Method == "GET" {
		b, err := json.Marshal(r.URL.Query())
		if err != nil {
			return nil, err
		}
		return p.Queryer.QueryRow("SELECT "+p.Map[r.Method]+"($1)", string(b)), nil
	}
	defer r.Body.Close()

	b, err := ioutil.ReadAll(io.LimitReader(r.Body, p.MaxBodySize))
	if err != nil {
		return nil, err
	}

	// just validate the json should be fast, see https://github.com/golang/go/issues/5683
	// var x struct{}
	// err = json.Unmarshal(b, &x)
	// improved performance, based on https://github.com/golang/go/issues/18086
	err = isValidJSON(b)
	if err != nil {
		return nil, err
	}

	return p.Queryer.QueryRow("SELECT "+p.Map[r.Method]+"($1)", string(b)), nil
}

func (p *PJ) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		row     *sql.Row
		code    int
		headers map[string]string
		b       []byte
		resp    map[string]interface{}
	)

steps:
	for jump := 1; err == nil; jump++ {
		switch jump - 1 {
		default:
			break steps
		case 0:
			if _, found := p.Map[r.Method]; !found {
				code = http.StatusMethodNotAllowed
				err = errors.New("no query found for method")
			} else {
				row, err = p.getRow(r)
			}
		case 1:
			b = []byte{}
			err = row.Scan(&b)
		case 2:
			resp = map[string]interface{}{}
			err = json.Unmarshal(b, &resp)
			if err != nil {
				code = http.StatusInternalServerError
			}
		case 3:
			if c, has := resp["http_status_code"]; has {
				delete(resp, "http_status_code")
				code, err = parseStatusCode(c)
			}
		case 4:
			if c, has := resp["http_headers"]; has {
				delete(resp, "http_headers")
				headers, err = parseHeaders(c)
			}
		}
	}

	if err != nil {
		if p.errTracker != nil {
			p.errTracker(err, r)
		}
		if code == 0 {
			code = http.StatusBadRequest
		}
	} else {
		if code == 0 {
			code = http.StatusOK
		}
	}

	if len(headers) > 0 {
		for k, v := range headers {
			w.Header().Set(k, v)
		}
	}

	if len(b) == 0 {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	w.Write(b)
}

func parseStatusCode(v interface{}) (code int, err error) {
	f, ok := v.(float64)
	if !ok {
		err = errors.New("http_error_code is not a float64")
		return
	}
	return int(f), nil
}

func parseHeaders(v interface{}) (headers map[string]string, err error) {
	h, ok := v.(map[string]interface{})
	if !ok {
		// fmt.Printf("%#v (%T)\n", h, h)
		err = errors.New("http_headers is not a map[string]string")
		return
	}

	r := map[string]string{}

	for k, v := range h {
		r[k] = fmt.Sprintf("%v", v)
	}

	return r, nil
}

// QueryRow requeries the given function qFn with the given parameter jsonParam and scans the result into the target
func QueryRow(q Queryer, qFn string, jsonParam string, target interface{}) error {
	r := q.QueryRow("SELECT "+qFn+"($1)", jsonParam)
	b := []byte{}
	err := r.Scan(&b)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}

type Muxer interface {
	// Handle registers a http.Handler for a path
	// If called twice for the same path, it must update the handler for the path
	Handle(path string, h http.Handler)

	// RemoveHandler unregisters the http.Handler for a given path
	RemoveHandler(path string)
}

type QueryCollection struct {
	RootDir    string
	Queries    map[string]map[string]string
	Handlers   map[string]*PJ
	errTracker func(error, *http.Request)
	*sync.Mutex
}

func NewQueryCollection(rootDir string, errTracker func(error, *http.Request)) (*QueryCollection, error) {
	pattern := filepath.Join(rootDir, "[a-z][a-z_0-9]*", "[a-z][a-z]*", "[a-z][a-z_0-9]*.sql")
	// fmt.Printf("pattern: %#v\n", pattern)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	fmt.Printf("files: %#v\n", files)

	var queries = map[string]map[string]string{}

	for _, f := range files {
		rel, err := filepath.Rel(rootDir, f)
		if err != nil {
			fmt.Printf("Err filepath Rel: %s\n", rel)
			return nil, err
		}

		var mntp, meth, fname string

		mntp, meth, fname, err = splitRelPath(rel)
		if err != nil {
			fmt.Printf("Err splitRelPath Rel: %s\n", rel)
			return nil, err
		}

		if _, has := queries[mntp]; !has {
			queries[mntp] = map[string]string{}
		}

		if _, has := queries[mntp][meth]; has {
			return nil, errors.New("more than one query function for " + meth + " /" + mntp)
		}

		queries[mntp][meth] = fname
	}

	return &QueryCollection{rootDir, queries, map[string]*PJ{}, errTracker, &sync.Mutex{}}, nil
}

func (q *QueryCollection) EachFile(fn func(filepath, funcname, meth string)) {
	for mntp, m := range q.Queries {
		for meth, fname := range m {
			fn(filepath.Join(q.RootDir, mntp, strings.ToLower(meth), fname+".sql"), fname, strings.ToLower(meth))
		}
	}
}

// RegisterQueryFuncs reads  the content of all query function files and
// execs them on the db
func (q *QueryCollection) RegisterQueryFuncs(db DB) (err error) {
	q.Lock()
	defer q.Unlock()
	q.EachFile(func(filepath, funcname, meth string) {
		if err != nil {
			return
		}
		var c []byte

		fmt.Printf("initial reading of file: %#v\n", filepath)

		c, err = ioutil.ReadFile(filepath)
		if err != nil {
			return
		}

		_, err = db.Exec(Sql(meth, funcname, c))

		if err != nil {
			return
		}

	})

	return
}

func (q *QueryCollection) RegisterHTTPHandlers(mux Muxer, db Queryer, maxBodySize int64) (err error) {
	if maxBodySize < 0 {
		maxBodySize = 2048 // default
	}
	q.Lock()
	defer q.Unlock()
	for mntp, m := range q.Queries {
		h := New(db, m, q.errTracker)
		h.MaxBodySize = maxBodySize
		q.Handlers[mntp] = h
		mux.Handle(mntp, h)
	}
	return nil
}

func (q *QueryCollection) RemoveQuery(mux Muxer, db DB, relpath string) error {
	fmt.Printf("RemoveQuery called\n")
	q.Lock()
	defer q.Unlock()
	mntp, meth, fname, err := splitRelPath(relpath)
	if err != nil {
		return err
	}

	pj, haspj := q.Handlers[mntp]
	if !haspj {
		return errors.New("query function for " + meth + "/" + mntp + " has no http handler")
	}

	_, hashtm := pj.Map[meth]
	if !hashtm {
		return errors.New("http.Handler for " + "/" + mntp + " does not handle " + meth)
	}

	m, hasm := q.Queries[mntp]
	if !hasm {
		return errors.New("query for " + meth + "/" + mntp + " does not exist")
	}

	qfn, has := m[meth]
	if !has {
		return errors.New("query for " + meth + "/" + mntp + " does not exist")
	}

	if qfn != fname {
		return errors.New("query function for " + meth + "/" + mntp + " has not the name " + fname)
	}

	sql := fmt.Sprintf("DROP FUNCTION %s(json)", fname)
	fmt.Printf("running: %#v\n", sql)
	_, err = db.Exec(sql)
	if err != nil {
		return err
	}

	if len(m) == 1 {
		delete(q.Queries, mntp)
	} else {
		delete(m, meth)
	}

	if len(pj.Map) == 1 {
		delete(q.Handlers, mntp)
		mux.RemoveHandler(mntp)
	} else {
		delete(pj.Map, meth)
		mux.Handle(mntp, pj)
	}
	return nil

}

func (q *QueryCollection) UpdateQuery(mux Muxer, db DB, relpath string) error {
	q.Lock()
	defer q.Unlock()
	fmt.Printf("UpdateQuery called\n")
	mntp, meth, fname, err := splitRelPath(relpath)
	if err != nil {
		return err
	}

	f := filepath.Join(q.RootDir, relpath)

	pj, haspj := q.Handlers[mntp]
	if !haspj {
		return errors.New("query function for " + meth + "/" + mntp + " has no http handler")
	}

	_, hashtm := pj.Map[meth]
	if !hashtm {
		return errors.New("http.Handler for " + "/" + mntp + " does not handle " + meth)
	}

	m, hasm := q.Queries[mntp]
	if !hasm {
		return errors.New("query for " + meth + "/" + mntp + " does not exist")
	}

	qfn, has := m[meth]
	if !has {
		return errors.New("query for " + meth + "/" + mntp + " does not exist")
	}

	if qfn != fname {
		return errors.New("query function for " + meth + "/" + mntp + " has not the name " + fname)
	}

	var c []byte

	c, err = ioutil.ReadFile(f)
	if err != nil {
		return err
	}
	_, err = db.Exec(Sql(meth, fname, c))
	if err != nil {
		return err
	}

	return nil
}

func checkRelPath(p string) error {
	return nil
	pattern := filepath.Join("[a-z][a-z_0-9]*", "[a-z][a-z]*", "[a-z][a-z_0-9]*.sql")
	// pattern = filepath.Join("[a-z][a-z_0-9]*")
	// p = "/" + p
	// fmt.Printf("trying to match %#v against the pattern %#v\n", p, pattern)
	is, err := filepath.Match("/"+pattern, "/"+p)
	if err != nil {
		panic(err.Error())
	}
	if !is {
		return errors.New("invalid path")
	}
	return nil
}

func withoutExt(file string) string {
	idx := strings.LastIndex(file, ".")
	if idx == -1 {
		return file
	}
	return file[:idx]
}

func splitRelPath(p string) (mntp string, meth string, fname string, err error) {
	err = checkRelPath(p)
	if err != nil {
		return
	}

	parr := strings.Split(p, string(filepath.Separator))

	// fmt.Printf("parr: %#v\n", parr)
	if len(parr) != 3 {
		err = errors.New("invalid path")
		return
	}

	mntp, meth, fname = parr[0], parr[1], parr[2]
	switch meth {
	case "get", "post", "put", "patch", "delete":
	default:
		err = errors.New("method " + meth + " is not allowed")
		return
	}
	meth = strings.ToUpper(meth)
	fname = withoutExt(fname)
	return
}

// AddQuery adds a query that is a file located in the path relative to the rootdir
func (q *QueryCollection) AddQuery(mux Muxer, db DB, relpath string) error {
	fmt.Printf("AddQuery called\n")
	q.Lock()
	defer q.Unlock()
	mntp, meth, fname, err := splitRelPath(relpath)
	if err != nil {
		return err
	}

	// fmt.Printf("mntp: %#v, meth: %#v, fname: %#v\n", mntp, meth, fname)

	f := filepath.Join(q.RootDir, relpath)

	if m, hasm := q.Queries[mntp]; hasm {
		if _, has := m[meth]; has {
			return errors.New("query function for " + meth + " /" + mntp + " already exists")
		}

		pj, haspj := q.Handlers[mntp]
		if !haspj {
			return errors.New("query function for " + meth + "/" + mntp + " has no http handler")
		}

		/*
			_, hashtm := pj.Map[meth]
			if !hashtm {
				return errors.New("http.Handler for " + "/" + mntp + " does not handle " + meth)
			}
		*/

		var c []byte

		c, err = ioutil.ReadFile(f)
		if err != nil {
			return err
		}
		_, err = db.Exec(Sql(meth, fname, c))
		if err != nil {
			return err
		}

		pj.Map[meth] = fname

		mux.Handle(mntp, pj)
		return nil
	}

	var c []byte

	c, err = ioutil.ReadFile(f)
	if err != nil {
		return err
	}
	_, err = db.Exec(Sql(meth, fname, c))
	if err != nil {
		return err
	}

	m := map[string]string{meth: fname}
	q.Queries[mntp] = m

	pj := New(db, m, q.errTracker)
	q.Handlers[mntp] = pj
	mux.Handle(mntp, pj)
	return nil
}

// LoadQueries loads queries from a filesystem and registers http handlers for them
// It expects the following directory structure of rootDir:
//
//    [mountpath]/[method]/[queryfn].sql
//
// for example: persons/get/all_persons.sql
//
// [mountpath] must be a single path segment (directory) and match the regexp [a-z][a-z_0-9]+.
// [method] must be the http request method, i.e. one of "get", "put", "patch", "delete", "post"
// [queryfn] must match the regexp [a-z][a-z_0-9]+ and is the name of the postgresql function
// the content of [queryfn].sql is the sql that is transferred to the database when the query is registered
// E.g.: If the filename is all_persons.sql the content must be something like
//
//     CREATE OR REPLACE FUNCTION all_persons(params json) RETURNS text AS $function$
//	   var o = {};
//     /* do your thing */
//     return JSON.stringify(o);
//     $function$ LANGUAGE plv8 IMMUTABLE STRICT;
//
// mux is the Muxer that is used to register the http.Handlers serving the queries
// NewQueryCollection(rootDir string, errTracker func(error, *http.Request)) (*QueryCollection, error)
func LoadQueries(rootDir string, mux Muxer, db DB, maxBodySize int64, errTracker func(error, *http.Request)) (*QueryCollection, error) {
	qc, err := NewQueryCollection(rootDir, errTracker)

	if err != nil {
		return nil, err
	}

	err = qc.RegisterQueryFuncs(db)

	if err != nil {
		return nil, err
	}

	err = qc.RegisterHTTPHandlers(mux, db, maxBodySize)
	if err != nil {
		return nil, err
	}

	return qc, nil
}

func Sql(meth, fname string, fbody []byte) string {
	return fmt.Sprintf(`
CREATE OR REPLACE FUNCTION pj__%s__%s(params json) RETURNS text AS $function$
	if (typeof params != 'object')
		return NULL;
  
  var response = {};
  response["http_status_code"] = 200;
	response["results"] = [];
	%s
  return JSON.stringify(response);

$function$ LANGUAGE plv8 IMMUTABLE STRICT;
`, fname, meth, string(fbody))
}
