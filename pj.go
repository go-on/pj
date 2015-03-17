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

7. Authentication and authorization will be handled by middleware surrounding the http.Handler return from pj.New

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
	"io/ioutil"
	"net/http"
)

type Queryer interface {
	QueryRow(sql string, args ...interface{}) *sql.Row
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
func New(db Queryer, m map[string]string, errTracker func(error, *http.Request)) PJ {
	for meth := range m {
		switch meth {
		case "GET", "POST", "PUT", "PATCH", "DELETE":
		default:
			panic("method " + meth + " is not allowed")
		}
	}

	return PJ{m, db, errTracker}
}

type PJ struct {
	queries    map[string]string
	Queryer    Queryer
	errTracker func(error, *http.Request)
}

func (p PJ) getRow(r *http.Request) (*sql.Row, error) {
	if r.Method == "GET" {
		b, err := json.Marshal(r.URL.Query())
		if err != nil {
			return nil, err
		}
		return p.Queryer.QueryRow("SELECT "+p.queries[r.Method]+"($1)", string(b)), nil
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	var v = map[string]interface{}{}
	err = json.Unmarshal(b, &v)
	if err != nil {
		return nil, err
	}
	return p.Queryer.QueryRow("SELECT "+p.queries[r.Method]+"($1)", string(b)), nil
}

func (p PJ) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
			if _, found := p.queries[r.Method]; !found {
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
				code, err = parseStatusCode(c)
			}
		case 4:
			if c, has := resp["http_headers"]; has {
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
