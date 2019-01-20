package builder

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	errInsertDataNotMatch = errors.New("insert data not match")
	errInsertNullData     = errors.New("insert null data")
	errOrderByParam       = errors.New("order param only should be ASC or DESC")
)

//the order of a map is unpredicatable so we need a sort algorithm to sort the fields
//and make it predicatable
var defaultSortAlgorithm = sort.Strings

//Comparable requires type implements the Build method
type Comparable interface {
	Build(placeHolderIndex *int) ([]string, []interface{})
}

type nilComparable byte

func (n nilComparable) Build(placeHolderIndex *int) ([]string, []interface{}) {
	return nil, nil
}

// Like means like
type Like map[string]interface{}

// Build implements the Comparable interface
func (l Like) Build(placeHolderIndex *int) ([]string, []interface{}) {
	if nil == l || 0 == len(l) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range l {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := l[cond[j]]
		*placeHolderIndex++
		cond[j] = cond[j] + " LIKE $" + fmt.Sprintf("%d", *placeHolderIndex)
		vals = append(vals, val)
	}
	return cond, vals
}

//Eq means equal(=)
type Eq map[string]interface{}

//Build implements the Comparable interface
func (e Eq) Build(placeHolderIndex *int) ([]string, []interface{}) {
	return build(e, "=", placeHolderIndex)
}

//Ne means Not Equal(!=)
type Ne map[string]interface{}

//Build implements the Comparable interface
func (n Ne) Build(placeHolderIndex *int) ([]string, []interface{}) {
	return build(n, "!=", placeHolderIndex)
}

//Lt means less than(<)
type Lt map[string]interface{}

//Build implements the Comparable interface
func (l Lt) Build(placeHolderIndex *int) ([]string, []interface{}) {
	return build(l, "<", placeHolderIndex)
}

//Lte means less than or equal(<=)
type Lte map[string]interface{}

//Build implements the Comparable interface
func (l Lte) Build(placeHolderIndex *int) ([]string, []interface{}) {
	return build(l, "<=", placeHolderIndex)
}

//Gt means greater than(>)
type Gt map[string]interface{}

//Build implements the Comparable interface
func (g Gt) Build(placeHolderIndex *int) ([]string, []interface{}) {
	return build(g, ">", placeHolderIndex)
}

//Gte means greater than or equal(>=)
type Gte map[string]interface{}

//Build implements the Comparable interface
func (g Gte) Build(placeHolderIndex *int) ([]string, []interface{}) {
	return build(g, ">=", placeHolderIndex)
}

//In means in
type In map[string][]interface{}

//Build implements the Comparable interface
func (i In) Build(placeHolderIndex *int) ([]string, []interface{}) {
	if nil == i || 0 == len(i) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range i {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := i[cond[j]]
		cond[j] = buildIn(cond[j], val, placeHolderIndex)
		vals = append(vals, val...)
	}
	return cond, vals
}

func buildIn(field string, vals []interface{}, placeHolderIndex *int) (cond string) {
	//cond = strings.TrimRight(strings.Repeat("?,", len(vals)), ",")
	// cond = "("
	for i := 0; i < len(vals); i++ {
		*placeHolderIndex++
		cond += fmt.Sprintf("$%d", *placeHolderIndex)
		if i != len(vals)-1 {
			cond += ","
		}
	}
	// cond += ")"
	cond = fmt.Sprintf("%s IN (%s)", quoteField(field), cond)
	return
}

func build(m map[string]interface{}, op string, placeHolderIndex *int) ([]string, []interface{}) {
	if nil == m || 0 == len(m) {
		return nil, nil
	}
	length := len(m)
	cond := make([]string, length)
	vals := make([]interface{}, length)
	var i int
	for key := range m {
		cond[i] = key
		i++
	}
	defaultSortAlgorithm(cond)
	for i = 0; i < length; i++ {
		vals[i] = m[cond[i]]
		*placeHolderIndex++
		cond[i] = assembleExpression(cond[i], op, placeHolderIndex)
	}
	return cond, vals
}

func assembleExpression(field, op string, placeHolderIndex *int) string {
	return quoteField(field) + op + "$" + fmt.Sprintf("%d", *placeHolderIndex)
}

func orderBy(orderMap []eleOrderBy) (string, error) {
	var orders []string
	for _, orderInfo := range orderMap {
		realOrder := strings.ToUpper(orderInfo.order)
		if realOrder != "ASC" && realOrder != "DESC" {
			return "", errOrderByParam
		}
		order := fmt.Sprintf("%s %s", quoteField(orderInfo.field), realOrder)
		orders = append(orders, order)
	}
	orderby := strings.Join(orders, ",")
	return orderby, nil
}

func resolveKV(m map[string]interface{}) (keys []string, vals []interface{}) {
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vals = append(vals, m[k])
	}
	return
}

func resolveFields(m map[string]interface{}) []string {
	var fields []string
	for k := range m {
		fields = append(fields, quoteField(k))
	}
	defaultSortAlgorithm(fields)
	return fields
}

func whereConnector(placeHolderIndex *int, conditions ...Comparable) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", nil
	}
	var where []string
	var values []interface{}
	for _, cond := range conditions {
		cons, vals := cond.Build(placeHolderIndex)
		if nil == cons {
			continue
		}
		where = append(where, cons...)
		values = append(values, vals...)
	}
	if 0 == len(where) {
		return "", nil
	}
	whereString := "(" + strings.Join(where, " AND ") + ")"
	return whereString, values
}

// deprecated
func quoteField(field string) string {
	return field
}

func buildInsert(table string, setMap []map[string]interface{}) (string, []interface{}, error) {
	format := "INSERT INTO %s (%s) VALUES %s"
	var fields []string
	var vals []interface{}
	if len(setMap) < 1 {
		return "", nil, errInsertNullData
	}
	fields = resolveFields(setMap[0])
	placeholder := "(" + strings.TrimRight(strings.Repeat("$%d,", len(fields)), ",") + ")"
	var sets []string
	for _, mapItem := range setMap {
		sets = append(sets, placeholder)
		for _, field := range fields {
			val, ok := mapItem[strings.Trim(field, "`")]
			if !ok {
				return "", nil, errInsertDataNotMatch
			}
			vals = append(vals, val)
		}
	}
	conds := fmt.Sprintf(format, quoteField(table), strings.Join(fields, ","), strings.Join(sets, ","))
	var holders []interface{}
	for i := 0; i < len(fields)*len(sets); i++ {
		holders = append(holders, i+1)
	}
	conds = fmt.Sprintf(conds, holders...)
	return conds, vals, nil
}

func buildUpdate(table string, update map[string]interface{}, conditions ...Comparable) (string, []interface{}, error) {
	var placeHolderIndex int
	format := "UPDATE %s SET %s"
	keys, vals := resolveKV(update)
	var sets string
	for _, k := range keys {
		placeHolderIndex++
		sets += fmt.Sprintf("%s=$%d,", quoteField(k), placeHolderIndex)
	}
	sets = strings.TrimRight(sets, ",")
	cond := fmt.Sprintf(format, quoteField(table), sets)
	whereString, whereVals := whereConnector(&placeHolderIndex, conditions...)
	if "" != whereString {
		cond = fmt.Sprintf("%s WHERE %s", cond, whereString)
		vals = append(vals, whereVals...)
	}
	return cond, vals, nil
}

func buildDelete(table string, conditions ...Comparable) (string, []interface{}, error) {
	var placeHolderIndex int
	whereString, vals := whereConnector(&placeHolderIndex, conditions...)
	if "" == whereString {
		return fmt.Sprintf("DELETE FROM %s", table), nil, nil
	}
	format := "DELETE FROM %s WHERE %s"

	cond := fmt.Sprintf(format, quoteField(table), whereString)
	return cond, vals, nil
}

func splitCondition(conditions []Comparable) ([]Comparable, []Comparable) {
	var having []Comparable
	var i int
	for i = len(conditions) - 1; i >= 0; i-- {
		if _, ok := conditions[i].(nilComparable); ok {
			break
		}
	}
	if i >= 0 && i < len(conditions)-1 {
		having = conditions[i+1:]
		return conditions[:i], having
	}
	return conditions, nil
}

func buildSelect(table string, ufields []string, groupBy string, uOrderBy []eleOrderBy, limit *eleLimit, conditions ...Comparable) (string, []interface{}, error) {
	var placeHolderIndex int
	format := "SELECT %s FROM %s"
	fields := "*"
	if len(ufields) > 0 {
		for i := range ufields {
			ufields[i] = quoteField(ufields[i])
		}
		fields = strings.Join(ufields, ",")
	}
	cond := fmt.Sprintf(format, fields, quoteField(table))
	where, having := splitCondition(conditions)
	whereString, vals := whereConnector(&placeHolderIndex, where...)
	if "" != whereString {
		cond = fmt.Sprintf("%s WHERE %s", cond, whereString)
	}
	if "" != groupBy {
		cond = fmt.Sprintf("%s GROUP BY %s", cond, quoteField(groupBy))
	}
	if nil != having {
		havingString, havingVals := whereConnector(&placeHolderIndex, having...)
		cond = fmt.Sprintf("%s HAVING %s", cond, havingString)
		vals = append(vals, havingVals...)
	}
	if len(uOrderBy) != 0 {
		str, err := orderBy(uOrderBy)
		if nil != err {
			return "", nil, err
		}
		cond = fmt.Sprintf("%s ORDER BY %s", cond, str)
	}
	if nil != limit {
		cond = fmt.Sprintf("%s LIMIT %d OFFSET %d", cond, limit.begin, limit.step)
	}
	return cond, vals, nil
}
